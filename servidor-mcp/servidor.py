"""Servidor MCP da Central de Salas (Streamable HTTP, stateless, revisao 2026-07-28).

Tres tools (listar_salas, consultar_disponibilidade, reservar_sala), um resource
(politica://uso) e o ciclo de MRTR na reserva: quando ha conflito, o resolver
`escolha_de_sala` devolve um `Elicit` e o SDK transforma isso em um resultado
`input_required` com o `requestState` selado (AES-256-GCM, chave derivada de
REQUEST_STATE_SECRET). O servidor nao guarda nada entre o input_required e o retry.

    REQUEST_STATE_SECRET=<64 hex> python3 servidor-mcp/servidor.py
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, Field, create_model

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.resolve import (
    AcceptedElicitation,
    Elicit,
    ElicitationResult,
    Resolve,
)
from mcp.server.request_state import RequestStateSecurity

RAIZ = Path(__file__).resolve().parent.parent
DADOS = RAIZ / "dados"
FUSO_SP = timezone(timedelta(hours=-3))
JANELA_INICIO = time(8, 0)
JANELA_FIM = time(20, 0)
DURACAO_MAXIMA = timedelta(hours=2)
MAX_ALTERNATIVAS = 3
VALIDADE_REQUEST_STATE = 10 * 60  # segundos; o enunciado pede entre 5 e 30 minutos

HOST = os.environ.get("MCP_HOST", "127.0.0.1")
PORTA = int(os.environ.get("MCP_PORT", "7301"))


def log(texto: str) -> None:
    """Todo log vai para o stderr (o logging do protocolo esta depreciado)."""
    print(texto, file=sys.stderr, flush=True)


# --------------------------------------------------------------------------
# Dominio: carregado dos JSON do starter, reservas novas ficam em memoria
# --------------------------------------------------------------------------

SALAS: list[dict] = json.loads((DADOS / "salas.json").read_text(encoding="utf-8"))
SALAS_POR_ID = {s["id"]: s for s in SALAS}
RESERVAS: list[dict] = json.loads((DADOS / "reservas.json").read_text(encoding="utf-8"))
POLITICA_TEXTO = (DADOS / "politica-de-uso.md").read_text(encoding="utf-8")
POLITICA_VERSAO = POLITICA_TEXTO.splitlines()[0].split(":", 1)[1].strip()

ERRO_SALA = "Sala inexistente: {sala}"
ERRO_JANELA = "Fora da janela de uso: a politica permite reservas entre 08:00 e 20:00"
ERRO_DURACAO = "Duracao acima do limite: a politica permite no maximo 2 horas"
ERRO_INTERVALO = "Intervalo invalido: fim deve ser posterior a inicio"
ERRO_SEM_ALTERNATIVA = "Sem alternativas disponiveis no intervalo"


def _proximo_id() -> str:
    maior = max((int(r["id"].split("-")[1]) for r in RESERVAS), default=0)
    return f"res-{maior + 1:04d}"


def _data(valor: str) -> datetime:
    try:
        dt = datetime.fromisoformat(valor)
    except ValueError as e:
        raise ToolError(f"Data invalida: {valor}") from e
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=FUSO_SP)
    return dt


def validar(sala: str, inicio: str, fim: str) -> tuple[datetime, datetime]:
    """As mesmas validacoes para consulta e reserva, na ordem: sala, intervalo, janela, duracao."""
    if sala not in SALAS_POR_ID:
        raise ToolError(ERRO_SALA.format(sala=sala))
    ini, fi = _data(inicio), _data(fim)
    if fi <= ini:
        raise ToolError(ERRO_INTERVALO)
    ini_sp, fim_sp = ini.astimezone(FUSO_SP), fi.astimezone(FUSO_SP)
    dentro = (
        ini_sp.date() == fim_sp.date()
        and JANELA_INICIO <= ini_sp.time()
        and fim_sp.time() <= JANELA_FIM
    )
    if not dentro:
        raise ToolError(ERRO_JANELA)
    if fi - ini > DURACAO_MAXIMA:
        raise ToolError(ERRO_DURACAO)
    return ini, fi


def conflitos(sala: str, ini: datetime, fi: datetime) -> list[dict]:
    return [
        r
        for r in RESERVAS
        if r["sala"] == sala and _data(r["inicio"]) < fi and ini < _data(r["fim"])
    ]


def alternativas(sala: str, ini: datetime, fi: datetime) -> list[str]:
    """Salas livres no intervalo com capacidade >= a pedida, no maximo 3, por (capacidade, id)."""
    minima = SALAS_POR_ID[sala]["capacidade"]
    livres = [
        s
        for s in SALAS
        if s["id"] != sala and s["capacidade"] >= minima and not conflitos(s["id"], ini, fi)
    ]
    livres.sort(key=lambda s: (s["capacidade"], s["id"]))
    return [s["id"] for s in livres[:MAX_ALTERNATIVAS]]


# --------------------------------------------------------------------------
# Modelos de saida (viram outputSchema + structuredContent)
# --------------------------------------------------------------------------


class SalaOut(BaseModel):
    id: str
    nome: str
    capacidade: int
    recursos: list[str]


class ListaDeSalas(BaseModel):
    salas: list[SalaOut]


class ConflitoOut(BaseModel):
    id: str
    inicio: str
    fim: str
    responsavel: str


class Disponibilidade(BaseModel):
    sala: str
    livre: bool
    conflitos: list[ConflitoOut]


class ReservaOut(BaseModel):
    reserva: str | None = None
    reservado: bool = True
    sala: str | None = None
    inicio: str | None = None
    fim: str | None = None
    responsavel: str | None = None
    politica: str | None = None
    motivo: str | None = None


class Escolha(BaseModel):
    """Forma base da resposta da elicitation; o enum real e montado por pedido."""

    sala: str


# --------------------------------------------------------------------------
# Servidor
# --------------------------------------------------------------------------


def _seguranca_request_state() -> RequestStateSecurity:
    segredo = os.environ.get("REQUEST_STATE_SECRET", "")
    if len(segredo.encode()) < 32:
        raise SystemExit(
            "REQUEST_STATE_SECRET ausente ou curto (minimo 32 bytes). Gere com:\n"
            '  python3 -c "import secrets; print(secrets.token_hex(32))"'
        )
    return RequestStateSecurity(keys=[segredo], ttl=VALIDADE_REQUEST_STATE)


mcp = MCPServer(
    name="central-de-salas",
    version="1.0.0",
    request_state_security=_seguranca_request_state(),
)


@mcp.tool(description="Lista todas as salas com capacidade e recursos.")
def listar_salas() -> ListaDeSalas:
    return ListaDeSalas(salas=[SalaOut(**s) for s in SALAS])


@mcp.tool(description="Diz se uma sala esta livre no intervalo, e quais reservas conflitam.")
def consultar_disponibilidade(sala: str, inicio: str, fim: str) -> Disponibilidade:
    ini, fi = validar(sala, inicio, fim)
    em_conflito = conflitos(sala, ini, fi)
    return Disponibilidade(
        sala=sala,
        livre=not em_conflito,
        conflitos=[
            ConflitoOut(id=r["id"], inicio=r["inicio"], fim=r["fim"], responsavel=r["responsavel"])
            for r in em_conflito
        ],
    )


def escolha_de_sala(sala: str, inicio: str, fim: str) -> Elicit[Escolha] | None:
    """Resolver do MRTR: decide se a reserva precisa perguntar algo ao cliente.

    Roda de novo a cada rodada (o servidor nao guarda estado). Sem conflito,
    devolve None e a tool reserva a sala pedida. Com conflito, devolve um
    `Elicit` com o enum das alternativas; o SDK responde `input_required` e
    sela no `requestState` a resposta que vier no retry.
    """
    ini, fi = validar(sala, inicio, fim)
    if not conflitos(sala, ini, fi):
        return None
    opcoes = alternativas(sala, ini, fi)
    if not opcoes:
        raise ToolError(ERRO_SEM_ALTERNATIVA)
    schema = create_model(
        "EscolhaDeSala",
        __base__=Escolha,
        sala=(
            Literal[tuple(opcoes)],  # type: ignore[valid-type]
            Field(title="Sala", description="Sala alternativa escolhida"),
        ),
    )
    return Elicit("A sala pedida esta ocupada nesse intervalo. Escolha uma alternativa.", schema)


@mcp.tool(description="Reserva uma sala. Se o intervalo estiver ocupado, pergunta qual alternativa usar.")
def reservar_sala(
    sala: str,
    inicio: str,
    fim: str,
    responsavel: str,
    escolha: Annotated[ElicitationResult[Escolha], Resolve(escolha_de_sala)],
) -> ReservaOut:
    if not isinstance(escolha, AcceptedElicitation):
        # decline ou cancel: conclui normalmente, sem reservar e sem isError
        return ReservaOut(reservado=False, motivo="recusado")
    destino = sala if escolha.data is None else escolha.data.sala
    ini, fi = validar(destino, inicio, fim)
    if conflitos(destino, ini, fi):
        raise ToolError(f"Sala ocupada no intervalo: {destino}")
    reserva = {
        "id": _proximo_id(),
        "sala": destino,
        "inicio": inicio,
        "fim": fim,
        "responsavel": responsavel,
    }
    RESERVAS.append(reserva)
    log(f"[reserva] {reserva['id']} sala={destino} {inicio}..{fim} responsavel={responsavel}")
    return ReservaOut(
        reserva=reserva["id"],
        sala=destino,
        inicio=inicio,
        fim=fim,
        responsavel=responsavel,
        politica=POLITICA_VERSAO,
    )


@mcp.resource("politica://uso", name="politica-de-uso", mime_type="text/markdown",
              description="Politica de uso das salas. A primeira linha declara a versao.")
def politica_de_uso() -> str:
    return POLITICA_TEXTO


# --------------------------------------------------------------------------
# Log por request no stderr: metodo, id e traceparent (ASGI, antes de qualquer
# validacao, para registrar tambem os requests que o transporte recusa)
# --------------------------------------------------------------------------


class LogDeRequests:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") != "POST":
            return await self.app(scope, receive, send)
        partes: list[bytes] = []
        while True:
            msg = await receive()
            partes.append(msg.get("body", b""))
            if not msg.get("more_body"):
                break
        corpo = b"".join(partes)
        status: dict = {}
        try:
            dados = json.loads(corpo)
        except ValueError:
            dados = None
        entregue = False

        async def receive_replay():
            nonlocal entregue
            if not entregue:
                entregue = True
                return {"type": "http.request", "body": corpo, "more_body": False}
            return await receive()

        async def send_espiao(message):
            if message["type"] == "http.response.start":
                status["http"] = message["status"]
            await send(message)

        await self.app(scope, receive_replay, send_espiao)
        if isinstance(dados, dict):
            params = dados.get("params") if isinstance(dados.get("params"), dict) else {}
            meta = params.get("_meta") if isinstance(params.get("_meta"), dict) else {}
            extra = ""
            if dados.get("method") == "tools/call":
                extra = f" name={params.get('name')}"
                if params.get("requestState"):
                    extra += " retry=sim"
            elif dados.get("method") == "resources/read":
                extra = f" uri={params.get('uri')}"
            caps = json.dumps(meta.get("io.modelcontextprotocol/clientCapabilities"), separators=(",", ":"))
            log(
                f"[mcp] method={dados.get('method')} id={dados.get('id')}{extra} "
                f"traceparent={meta.get('traceparent', '-')} capabilities={caps} http={status.get('http')}"
            )


def main() -> None:
    import uvicorn

    log(f"[mcp] central-de-salas em http://{HOST}:{PORTA}/mcp (politica {POLITICA_VERSAO})")
    app = mcp.streamable_http_app(
        streamable_http_path="/mcp",
        json_response=True,
        stateless_http=True,
        host=HOST,
    )
    uvicorn.run(LogDeRequests(app), host=HOST, port=PORTA, log_level="warning")


if __name__ == "__main__":
    main()
