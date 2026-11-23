"""Agente Central de Salas: host MCP por dentro, servidor A2A v1.0 por fora.

Por fora, publica o Agent Card e atende SendMessage/GetTask no binding JSON-RPC
em /a2a. Por dentro, e cliente MCP (SDK oficial, revisao 2026-07-28) do
servidor-mcp: descobre as tools com tools/list, le o resource politica://uso e
chama reservar_sala vendo o resultado `input_required` cru.

A ponte esta em `Agente._traduzir_resultado` (input_required -> Task em
TASK_STATE_INPUT_REQUIRED, requestState guardado por Task) e em
`Agente._retomar` (escolha do cliente A2A -> retry do tools/call com id novo,
inputResponses e o requestState ecoado sem modificacao).

Nao ha LLM: o pedido tem formato fixo e as decisoes sao por regra.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import uvicorn
from mcp import Client, ClientSession
from mcp.shared.exceptions import MCPError
from mcp_types import (
    CallToolResult,
    ClientCapabilities,
    ElicitationCapability,
    ElicitResult,
    FormElicitationCapability,
    Implementation,
    InputRequiredResult,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

HOST = os.environ.get("AGENTE_HOST", "127.0.0.1")
PORTA = int(os.environ.get("AGENTE_PORT", "7300"))
URL_PUBLICA = os.environ.get("AGENTE_URL", f"http://{HOST}:{PORTA}").rstrip("/")
URL_MCP = os.environ.get("MCP_URL", "http://127.0.0.1:7301/mcp")
PROTOCOLO_MCP = "2026-07-28"
URI_POLITICA = "politica://uso"
TOOL_RESERVA = "reservar_sala"

ESTADOS_TERMINAIS = {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED"}

# Erros JSON-RPC / A2A v1.0
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
TASK_NOT_FOUND = -32001
UNSUPPORTED_OPERATION = -32004

RE_TRACEPARENT = re.compile(r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$")
RE_PEDIDO = re.compile(r"^\s*reservar\s+(.*)$", re.IGNORECASE | re.DOTALL)
RE_PAR = re.compile(r"(\w+)=(.*?)(?=\s+\w+=|\s*$)", re.DOTALL)
RE_ESCOLHA = re.compile(r"^\s*escolha=(\S+)\s*$")


def log(texto: str) -> None:
    print(texto, file=sys.stderr, flush=True)


def novo_id(prefixo: str) -> str:
    return f"{prefixo}-{secrets.token_hex(6)}"


# --------------------------------------------------------------------------
# Cliente MCP
# --------------------------------------------------------------------------


class SessaoMCP(ClientSession):
    """Declara exatamente {"elicitation": {"form": {}}} em todo request.

    O SDK so declara elicitation quando recebe um elicitation_callback, e ai
    declara form e url, e o callback responderia a pergunta sozinho. O agente
    nao quer isso: a pergunta tem de voltar crua para o cliente A2A. Por isso a
    capability e declarada aqui, sem callback, e so em form mode.
    """

    def _build_capabilities(self, version: str) -> ClientCapabilities:
        caps = super()._build_capabilities(version)
        caps.elicitation = ElicitationCapability(form=FormElicitationCapability())
        return caps


class ClienteMCP(Client):
    async def _build_session(self, exit_stack):  # type: ignore[override]
        sessao = await super()._build_session(exit_stack)
        sessao.__class__ = SessaoMCP
        return sessao


# --------------------------------------------------------------------------
# Estado das Tasks (em memoria, no processo do agente)
# --------------------------------------------------------------------------


@dataclass
class Pausa:
    """O que o agente guarda enquanto a Task espera o cliente A2A.

    `request_state` e opaco: so e guardado e ecoado, nunca aberto. Nunca sai
    deste objeto para uma resposta A2A.
    """

    tool: str
    argumentos: dict[str, Any]
    chave: str
    campo: str
    opcoes: list[str]
    request_state: str | None


@dataclass
class Tarefa:
    id: str
    context_id: str
    estado: str = "TASK_STATE_SUBMITTED"
    mensagem_status: dict | None = None
    historico: list[dict] = field(default_factory=list)
    artifacts: list[dict] = field(default_factory=list)
    trace_id: str = ""
    trace_flags: str = "01"
    politica: str | None = None
    pausa: Pausa | None = None
    trava: asyncio.Lock = field(default_factory=asyncio.Lock)

    def para_wire(self) -> dict:
        status: dict[str, Any] = {"state": self.estado}
        if self.mensagem_status is not None:
            status["message"] = self.mensagem_status
        return {
            "id": self.id,
            "contextId": self.context_id,
            "status": status,
            "history": self.historico,
            "artifacts": self.artifacts,
        }


class ErroRPC(Exception):
    def __init__(self, codigo: int, mensagem: str, dados: Any = None, http: int = 200):
        super().__init__(mensagem)
        self.codigo, self.mensagem, self.dados, self.http = codigo, mensagem, dados, http


# --------------------------------------------------------------------------
# O agente
# --------------------------------------------------------------------------


class Agente:
    def __init__(self, cliente: Client) -> None:
        self.mcp = cliente
        self.tarefas: dict[str, Tarefa] = {}

    # ---- utilidades de Task -------------------------------------------------

    def _transitar(self, tarefa: Tarefa, estado: str, texto: str | None = None) -> None:
        if tarefa.estado in ESTADOS_TERMINAIS:
            raise ErroRPC(UNSUPPORTED_OPERATION, f"Task {tarefa.id} ja esta em estado terminal ({tarefa.estado})")
        tarefa.estado = estado
        if texto is not None:
            msg = {
                "messageId": novo_id("msg"),
                "role": "ROLE_AGENT",
                "parts": [{"text": texto}],
                "taskId": tarefa.id,
                "contextId": tarefa.context_id,
            }
            tarefa.mensagem_status = msg
            tarefa.historico.append(msg)
        log(f"[a2a] task={tarefa.id} estado={estado}" + (f" msg={texto!r}" if texto else ""))

    def _traceparent(self, tarefa: Tarefa) -> str:
        # mesmo trace-id da Task, span-id novo a cada request MCP
        return f"00-{tarefa.trace_id}-{secrets.token_hex(8)}-{tarefa.trace_flags}"

    def _meta(self, tarefa: Tarefa) -> dict[str, Any]:
        return {"traceparent": self._traceparent(tarefa)}

    @staticmethod
    def _adotar_trace(tarefa: Tarefa, cabecalho: str | None) -> None:
        m = RE_TRACEPARENT.match((cabecalho or "").strip().lower())
        if m and m.group(2) != "0" * 32:
            tarefa.trace_id, tarefa.trace_flags = m.group(2), m.group(4)
        elif not tarefa.trace_id:
            tarefa.trace_id = secrets.token_hex(16)

    # ---- A2A: SendMessage / GetTask ----------------------------------------

    async def send_message(self, params: dict, traceparent: str | None) -> dict:
        mensagem = params.get("message")
        if not isinstance(mensagem, dict):
            raise ErroRPC(INVALID_PARAMS, "params.message e obrigatorio")
        partes = mensagem.get("parts")
        if not isinstance(partes, list) or not partes:
            raise ErroRPC(INVALID_PARAMS, "message.parts e obrigatorio")
        texto = " ".join(p.get("text", "") for p in partes if isinstance(p, dict)).strip()

        task_id = mensagem.get("taskId")
        if task_id:
            tarefa = self.tarefas.get(task_id)
            if tarefa is None:
                raise ErroRPC(TASK_NOT_FOUND, f"Task nao encontrada: {task_id}")
            async with tarefa.trava:
                if tarefa.estado in ESTADOS_TERMINAIS:
                    raise ErroRPC(
                        UNSUPPORTED_OPERATION,
                        f"Task {tarefa.id} ja esta em estado terminal ({tarefa.estado}) e nao aceita novas mensagens",
                    )
                if tarefa.estado != "TASK_STATE_INPUT_REQUIRED" or tarefa.pausa is None:
                    raise ErroRPC(UNSUPPORTED_OPERATION, f"Task {tarefa.id} nao esta aguardando entrada")
                self._adotar_trace(tarefa, traceparent)
                tarefa.historico.append(mensagem)
                await self._retomar(tarefa, texto)
                return {"task": tarefa.para_wire()}

        tarefa = Tarefa(id=novo_id("task"), context_id=mensagem.get("contextId") or novo_id("ctx"))
        self._adotar_trace(tarefa, traceparent)
        self.tarefas[tarefa.id] = tarefa
        async with tarefa.trava:
            tarefa.historico.append(mensagem)
            log(f"[a2a] task={tarefa.id} estado=TASK_STATE_SUBMITTED trace-id={tarefa.trace_id}")
            self._transitar(tarefa, "TASK_STATE_WORKING")
            await self._iniciar(tarefa, texto)
        return {"task": tarefa.para_wire()}

    def get_task(self, params: dict) -> dict:
        task_id = params.get("id")
        if not task_id:
            raise ErroRPC(INVALID_PARAMS, "params.id e obrigatorio")
        tarefa = self.tarefas.get(task_id)
        if tarefa is None:
            raise ErroRPC(TASK_NOT_FOUND, f"Task nao encontrada: {task_id}")
        wire = tarefa.para_wire()
        tamanho = params.get("historyLength")
        if isinstance(tamanho, int) and tamanho >= 0:
            wire["history"] = wire["history"][-tamanho:] if tamanho else []
        return {"task": wire}

    # ---- Host MCP ------------------------------------------------------------

    async def _iniciar(self, tarefa: Tarefa, texto: str) -> None:
        pedido = self._interpretar(texto)
        if pedido is None:
            self._transitar(
                tarefa,
                "TASK_STATE_FAILED",
                "Pedido invalido: use reservar sala=<id> inicio=<iso8601> fim=<iso8601> responsavel=<nome>",
            )
            return
        try:
            # 1. descoberta em runtime: nada de lista fixa de tools no codigo
            listagem = await self.mcp.list_tools(meta=self._meta(tarefa))
            tool = next((t for t in listagem.tools if t.name == TOOL_RESERVA), None)
            if tool is None:
                self._transitar(tarefa, "TASK_STATE_FAILED", f"O servidor MCP nao oferece a tool {TOOL_RESERVA}")
                return
            esquema = tool.input_schema or {}
            propriedades = list((esquema.get("properties") or {}).keys())
            faltando = [c for c in esquema.get("required", []) if c not in pedido]
            if faltando:
                self._transitar(tarefa, "TASK_STATE_FAILED", f"Pedido invalido: faltam {', '.join(faltando)}")
                return
            argumentos = {c: pedido[c] for c in propriedades if c in pedido}

            # 2. resource: a aplicacao escolhe ler a politica e guarda a versao
            tarefa.politica = await self._versao_politica(tarefa)

            # 3. tools/call vendo o input_required cru
            resultado = await self.mcp.session.call_tool(
                tool.name, argumentos, meta=self._meta(tarefa), allow_input_required=True
            )
        except MCPError as e:
            self._transitar(tarefa, "TASK_STATE_FAILED", f"Erro do servidor MCP: {e.error.message}")
            return
        self._traduzir_resultado(tarefa, tool.name, argumentos, resultado)

    async def _versao_politica(self, tarefa: Tarefa) -> str | None:
        lido = await self.mcp.read_resource(URI_POLITICA, meta=self._meta(tarefa))
        for conteudo in lido.contents:
            texto = getattr(conteudo, "text", None)
            if texto:
                primeira = texto.splitlines()[0]
                if primeira.lower().startswith("versao:"):
                    return primeira.split(":", 1)[1].strip()
        return None

    @staticmethod
    def _interpretar(texto: str) -> dict[str, str] | None:
        m = RE_PEDIDO.match(texto)
        if not m:
            return None
        pares = {k: v.strip() for k, v in RE_PAR.findall(m.group(1))}
        return pares or None

    # ---- A ponte -------------------------------------------------------------

    def _traduzir_resultado(
        self,
        tarefa: Tarefa,
        tool: str,
        argumentos: dict[str, Any],
        resultado: CallToolResult | InputRequiredResult | Any,
    ) -> None:
        """Traduz o resultado MCP em estado da Task A2A. Aqui o input_required vira pausa."""
        if isinstance(resultado, InputRequiredResult):
            pedidos = resultado.input_requests or {}
            if len(pedidos) != 1:
                self._transitar(tarefa, "TASK_STATE_FAILED", "O servidor MCP pediu uma entrada que o agente nao sabe repassar")
                return
            chave, pedido = next(iter(pedidos.items()))
            esquema = pedido.params.requested_schema if getattr(pedido, "params", None) else {}
            esquema = esquema if isinstance(esquema, dict) else esquema.model_dump(by_alias=True, exclude_none=True)
            obrigatorios = esquema.get("required") or list((esquema.get("properties") or {}).keys())
            campo = obrigatorios[0]
            definicao = (esquema.get("properties") or {}).get(campo, {})
            opcoes = list(definicao.get("enum") or ([definicao["const"]] if "const" in definicao else []))
            # requestState guardado ligado a esta Task, e so aqui
            tarefa.pausa = Pausa(
                tool=tool,
                argumentos=argumentos,
                chave=chave,
                campo=campo,
                opcoes=opcoes,
                request_state=resultado.request_state,
            )
            self._transitar(tarefa, "TASK_STATE_INPUT_REQUIRED", self._linha_alternativas(tarefa.pausa))
            return

        tarefa.pausa = None
        if not isinstance(resultado, CallToolResult):
            self._transitar(tarefa, "TASK_STATE_FAILED", "Resposta inesperada do servidor MCP")
            return
        if resultado.is_error:
            texto = " ".join(getattr(c, "text", "") for c in resultado.content).strip()
            self._transitar(tarefa, "TASK_STATE_FAILED", texto or "Erro na execucao da tool")
            return
        dados = resultado.structured_content or {}
        if dados.get("reservado") is False:
            self._transitar(tarefa, "TASK_STATE_CANCELED", f"Reserva nao realizada: {dados.get('motivo') or 'recusado'}")
            return
        reserva = {
            "reserva": dados.get("reserva"),
            "sala": dados.get("sala"),
            "inicio": dados.get("inicio"),
            "fim": dados.get("fim"),
            "responsavel": dados.get("responsavel"),
            "politica": tarefa.politica,
        }
        tarefa.artifacts.append(
            {"artifactId": novo_id("art"), "name": "reserva", "parts": [{"text": json.dumps(reserva, ensure_ascii=False)}]}
        )
        self._transitar(tarefa, "TASK_STATE_COMPLETED", f"Reserva {reserva['reserva']} confirmada na {reserva['sala']}.")

    @staticmethod
    def _linha_alternativas(pausa: Pausa) -> str:
        return "alternativas: " + ", ".join(pausa.opcoes)

    async def _retomar(self, tarefa: Tarefa, texto: str) -> None:
        """Continuacao da Task: vira o retry do tools/call original, com id novo."""
        pausa = tarefa.pausa
        assert pausa is not None
        m = RE_ESCOLHA.match(texto)
        valor = m.group(1) if m else None
        if valor == "recusar":
            resposta = ElicitResult(action="decline")
        elif valor is not None and valor in pausa.opcoes:
            resposta = ElicitResult(action="accept", content={pausa.campo: valor})
        else:
            # escolha fora do enum: a Task continua pausada e repete a lista
            self._transitar(tarefa, "TASK_STATE_INPUT_REQUIRED", self._linha_alternativas(pausa))
            return

        self._transitar(tarefa, "TASK_STATE_WORKING")
        try:
            # o SDK atribui um id de JSON-RPC novo a este request; o requestState vai
            # exatamente como veio, junto com a resposta na mesma chave do inputRequests
            resultado = await self.mcp.session.call_tool(
                pausa.tool,
                pausa.argumentos,
                input_responses={pausa.chave: resposta},
                request_state=pausa.request_state,
                meta=self._meta(tarefa),
                allow_input_required=True,
            )
        except MCPError as e:
            tarefa.pausa = None
            self._transitar(tarefa, "TASK_STATE_FAILED", f"Erro do servidor MCP: {e.error.message}")
            return
        self._traduzir_resultado(tarefa, pausa.tool, pausa.argumentos, resultado)


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------


def agent_card() -> dict:
    return {
        "name": "Central de Salas",
        "description": "Reserva salas de reuniao da Hill Valley Tech.",
        "provider": {"organization": "Hill Valley Tech", "url": "https://hillvalley.example"},
        "version": "1.0.0",
        "supportedInterfaces": [
            {"url": f"{URL_PUBLICA}/a2a", "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}
        ],
        "capabilities": {"streaming": False, "pushNotifications": False, "extendedAgentCard": False},
        "defaultInputModes": ["text/plain"],
        "defaultOutputModes": ["text/plain"],
        "skills": [
            {
                "id": "reservar-sala",
                "name": "Reservar sala",
                "description": "Reserva uma sala em um intervalo. Se houver conflito, pergunta qual alternativa usar.",
                "tags": ["salas", "agenda"],
                "inputModes": ["text/plain"],
                "outputModes": ["text/plain"],
                "examples": [
                    "reservar sala=sala-garagem inicio=2026-11-03T14:00:00-03:00 "
                    "fim=2026-11-03T15:00:00-03:00 responsavel=Marty"
                ],
            }
        ],
    }


def criar_app() -> Starlette:
    estado: dict[str, Agente] = {}

    @asynccontextmanager
    async def ciclo_de_vida(_app):
        cliente = ClienteMCP(
            URL_MCP,
            mode=PROTOCOLO_MCP,  # sem handshake nem probe: versao e capabilities vao em todo _meta
            client_info=Implementation(name="agente-central-de-salas", version="1.0.0"),
            cache=None,
        )
        async with cliente:
            estado["agente"] = Agente(cliente)
            log(f"[a2a] agente em {URL_PUBLICA}/a2a, servidor MCP em {URL_MCP}")
            yield

    async def card(_request: Request) -> JSONResponse:
        return JSONResponse(agent_card())

    async def rpc(request: Request) -> JSONResponse:
        id_rpc = None
        try:
            try:
                corpo = await request.json()
            except ValueError:
                raise ErroRPC(PARSE_ERROR, "JSON invalido")
            if not isinstance(corpo, dict) or corpo.get("jsonrpc") != "2.0" or "method" not in corpo:
                raise ErroRPC(INVALID_REQUEST, "Request JSON-RPC invalido")
            id_rpc = corpo.get("id")
            params = corpo.get("params") or {}
            if not isinstance(params, dict):
                raise ErroRPC(INVALID_PARAMS, "params deve ser um objeto")
            agente = estado["agente"]
            metodo = corpo["method"]
            log(f"[a2a] <- {metodo} id={id_rpc} traceparent={request.headers.get('traceparent', '-')}")
            if metodo == "SendMessage":
                resultado = await agente.send_message(params, request.headers.get("traceparent"))
            elif metodo == "GetTask":
                resultado = agente.get_task(params)
            else:
                raise ErroRPC(METHOD_NOT_FOUND, f"Metodo nao suportado: {metodo}")
            return JSONResponse({"jsonrpc": "2.0", "id": id_rpc, "result": resultado})
        except ErroRPC as e:
            erro: dict[str, Any] = {"code": e.codigo, "message": e.mensagem}
            if e.dados is not None:
                erro["data"] = e.dados
            log(f"[a2a] -> erro {e.codigo}: {e.mensagem}")
            return JSONResponse({"jsonrpc": "2.0", "id": id_rpc, "error": erro}, status_code=e.http)

    return Starlette(
        routes=[
            Route("/.well-known/agent-card.json", card, methods=["GET"]),
            Route("/a2a", rpc, methods=["POST"]),
        ],
        lifespan=ciclo_de_vida,
    )


def main() -> None:
    uvicorn.run(criar_app(), host=HOST, port=PORTA, log_level="warning")


if __name__ == "__main__":
    main()
