# A Ponte: um agente A2A com MCP por dentro

Entrega do desafio do módulo Desenvolvimento de Agentes com IA. Dois processos separados, em Python:

- `servidor-mcp/servidor.py`: servidor MCP (SDK oficial `mcp` 2.3.0, revisão 2026-07-28) em Streamable HTTP stateless, porta **7301**, endpoint `/mcp`. Três tools (`listar_salas`, `consultar_disponibilidade`, `reservar_sala`), o resource `politica://uso` e o ciclo de MRTR na reserva.
- `agente/agente.py`: o agente. Por dentro é host MCP (cliente do SDK oficial, falando HTTP com o servidor acima); por fora é servidor A2A v1.0 no binding JSON-RPC, porta **7300**, com o card em `/.well-known/agent-card.json` e o endpoint em `/a2a`.

O agente não usa LLM: o pedido tem formato fixo e cada decisão é por regra. Regra de sala (conflito, política, alternativas) fica só no servidor MCP.

## Como rodar

Pré-requisito: Python 3.10 ou superior.

```bash
git clone https://github.com/<seu-usuario>/desafio-a2a-com-mcp.git
cd desafio-a2a-com-mcp

python3 -m venv .venv
source .venv/bin/activate
pip install .            # instala só as dependências travadas no pyproject.toml
```

Gere a chave de integridade do `requestState` (mínimo de 32 bytes aleatórios) e exporte-a. **Não** coloque o valor no repositório:

```bash
export REQUEST_STATE_SECRET="$(python3 -c "import secrets; print(secrets.token_hex(32))")"
```

Terminal 1, servidor MCP (o stderr dele é o log de requests):

```bash
source .venv/bin/activate
export REQUEST_STATE_SECRET=...   # o mesmo valor gerado acima
python3 servidor-mcp/servidor.py
```

Terminal 2, agente:

```bash
source .venv/bin/activate
python3 agente/agente.py
```

Terminal 3, validador (com os dois processos recém-iniciados):

```bash
python3 validador/validar.py --agente http://localhost:7300 --mcp http://localhost:7301
```

O validador cria reservas. Para refazer os testes manuais do fluxo do avaliador (passo 7 em diante, que espera `alternativas: sala-fusca, sala-mirante`), reinicie os dois processos antes, porque as reservas vivem em memória.

Para reiniciar só o servidor MCP no meio de um ciclo de MRTR, use o mesmo `REQUEST_STATE_SECRET`: o `requestState` emitido antes do restart continua válido depois.

Variáveis opcionais (os padrões são os que o validador espera): `MCP_HOST`/`MCP_PORT` (127.0.0.1/7301), `AGENTE_HOST`/`AGENTE_PORT` (127.0.0.1/7300), `AGENTE_URL` (URL pública usada no card) e `MCP_URL` (http://127.0.0.1:7301/mcp).

## Onde a ponte acontece

A ponte está no agente, em dois pontos. O primeiro é `Agente._traduzir_resultado` (`agente/agente.py`, linha 311): o `tools/call` é feito com `allow_input_required=True`, então o agente recebe o `InputRequiredResult` cru, em vez de deixar o SDK responder a elicitation sozinho. Quando o resultado é `input_required` (linha 319), o agente lê a única entrada de `inputRequests`, guarda a chave, o enum do `requestedSchema` e o `requestState` em um objeto `Pausa` preso àquela Task (linha 332), e coloca a Task em `TASK_STATE_INPUT_REQUIRED` com a mensagem `alternativas: <ids>`, na ordem do enum. O segundo ponto é `Agente._retomar` (linha 372): quando chega o `SendMessage` com o mesmo `taskId` e `escolha=<valor>`, o agente monta o `inputResponses` na mesma chave (`accept` com a sala, ou `decline` para `escolha=recusar`) e repete o `tools/call` original com o `requestState` ecoado sem modificação (linhas 394 e 395). O SDK atribui um id de JSON-RPC novo a esse request; no stderr do servidor dá para ver o par, por exemplo `id=3` no pedido e `id=7` no retry. Uma escolha fora do enum não chega ao servidor: a Task continua pausada e repete a lista.

Do lado do servidor, o par correspondente é o resolver `escolha_de_sala` (`servidor-mcp/servidor.py`, linha 210). Ele roda a cada rodada: sem conflito devolve `None` e a tool reserva a sala pedida; com conflito calcula as alternativas e devolve um `Elicit` com enum dinâmico; sem alternativas levanta o erro de execução `Sem alternativas disponiveis no intervalo`. O SDK transforma o `Elicit` em `input_required`, recusa com `-32021` o cliente que não declarou elicitation em form mode, e no retry injeta a resposta selada na tool.

## Decisões técnicas

**Proteção do `requestState`.** Uso o utilitário do próprio SDK (`RequestStateSecurity` + `RequestStateBoundary`), que o `MCPServer` instala como middleware: o estado é cifrado e autenticado com AES-256-GCM, com chave derivada por HKDF-SHA256 de `REQUEST_STATE_SECRET` (o processo não sobe sem ela, nem com menos de 32 bytes, e não há segredo no código). Dentro do envelope selado vão a expiração, o método, o nome da tool, um digest dos argumentos e o público (nome do servidor). Assim:

- trocar um caractere do token quebra a tag GCM e o servidor responde `-32602 Invalid or expired requestState` (HTTP 400);
- reenviar o token com argumentos diferentes dos selados também é rejeitado com `-32602` (é o caminho "o servidor rejeita o estado" aceito pelo enunciado);
- como a chave vem do ambiente e o servidor não guarda nada entre as rodadas, um retry feito depois de reiniciar o processo funciona.

**Validade.** 10 minutos (`ttl=600` em `RequestStateSecurity`), dentro da faixa de 5 a 30 minutos pedida.

**Estado das Tasks.** Em memória, no processo do agente: um dicionário `tarefas` de `Tarefa` (id, contextId, estado, histórico e artifacts), com uma trava por Task para que duas continuações não corram juntas. O `requestState` fica só no campo `pausa` da Task, que nunca é serializado: `Tarefa.para_wire()` não o inclui, então ele não aparece no card, no artifact nem em mensagem. Por isso duas Tasks pausadas ao mesmo tempo não trocam de estado. Estado terminal é definitivo: `SendMessage` para Task `COMPLETED`, `CANCELED` ou `FAILED` recebe erro `-32004`.

**Host MCP.** O agente usa o `Client` do SDK com `mode="2026-07-28"` (sem handshake: versão e capabilities vão no `_meta` de cada request, junto com os headers `MCP-Protocol-Version`, `Mcp-Method` e `Mcp-Name` que o SDK espelha do corpo). Em cada Task ele faz `tools/list` (descoberta em runtime: os argumentos são montados a partir das `properties` do `inputSchema` descoberto), lê `politica://uso` para extrair a versão da primeira linha e só então chama a tool. Uma subclasse mínima da sessão (`SessaoMCP`) declara exatamente `{"elicitation": {"form": {}}}`: por padrão o SDK só declara elicitation quando recebe um callback, e nesse caso declara `form` e `url` e responde a pergunta sozinho, o que fecharia o ciclo sem nunca voltar ao cliente A2A.

**traceparent.** O trace-id do header `traceparent` da chamada A2A é guardado na Task e vai no `_meta.traceparent` de todo request MCP dela (`tools/list`, `resources/read`, `tools/call` e o retry), com span-id novo a cada request. Se a continuação não trouxer header, vale o trace-id da Task. O servidor registra no stderr, para cada request: método, id, nome da tool ou URI, `traceparent`, capabilities declaradas e o status HTTP.

**Erros de execução viram `FAILED`.** Um resultado com `isError: true` termina a Task em `TASK_STATE_FAILED`, com o texto da tool como mensagem de status e no histórico (por exemplo `Error executing tool reservar_sala: Sala inexistente: sala-delorean`; o prefixo é do SDK). Recusa (`decline`) termina em `TASK_STATE_CANCELED`.

## Saída do validador

Última execução, com os dois processos recém-iniciados:

```
trace-id desta execucao: c3429fa2c779df8890c5811edcd6d8ca
procure esse valor no stderr do servidor MCP para conferir a propagacao do traceparent.

PASS 01 tools/list traz as tres tools
PASS 02 toda tool tem inputSchema de objeto
PASS 03 listar_salas devolve structuredContent e o mesmo JSON em texto
PASS 04 _meta sem protocolVersion devolve -32602 e HTTP 400
PASS 05 _meta sem clientCapabilities devolve -32602 e HTTP 400
PASS 06 tool inexistente e recusada, por -32602 ou por isError
PASS 07 resources/read de politica://uso devolve a politica
PASS 08 resources/read de URI inexistente devolve -32602
PASS 09 sala inexistente devolve isError com a mensagem exata
PASS 10 fora da janela devolve isError com a mensagem exata
PASS 11 duracao acima de 2h devolve isError com a mensagem exata
PASS 12 intervalo invertido devolve isError com a mensagem exata
PASS 13 conflito devolve input_required com inputRequests e requestState
PASS 14 a elicitation e form mode e oferece as alternativas na ordem certa
PASS 15 conflito sem a capability elicitation devolve -32021 e HTTP 400
PASS 16 retry com inputResponses e requestState conclui a reserva
PASS 17 requestState adulterado e rejeitado com -32602
PASS 18 argumentos adulterados no retry nao tomam efeito
PASS 19 recusa conclui sem reservar e sem isError
PASS 20 conflito sem alternativa possivel devolve isError com a mensagem exata

PASS 21 agent card responde 200 no well-known com JSON
PASS 22 o card declara a interface JSON-RPC com url e versao 1.0
PASS 23 o card declara a skill reservar-sala
PASS 24 SendMessage com sala livre conclui a Task
PASS 25 o artifact chama reserva e traz a versao da politica
PASS 26 GetTask devolve id, contextId e estado corrente
PASS 27 SendMessage com sala ocupada pausa a Task
PASS 28 a Task pausada lista as alternativas na ordem certa
PASS 29 escolha fora do enum mantem a Task pausada
PASS 30 a continuacao conclui a Task na sala escolhida
PASS 31 SendMessage em Task terminal e recusado
PASS 32 a recusa termina a Task em CANCELED
PASS 33 duas Tasks pausadas ao mesmo tempo concluem cada uma com a sua reserva
PASS 34 nenhuma resposta A2A carrega o requestState
PASS 35 sala inexistente termina a Task em FAILED com a mensagem da tool
PASS 36 o agente e deterministico: o mesmo pedido produz a mesma pausa

resumo: 36 passaram, 0 falharam, de 36 verificacoes
```
