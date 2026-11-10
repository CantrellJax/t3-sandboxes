import { test } from "node:test";
import assert from "node:assert/strict";
import { Guest } from "../src/mcp.mjs";
test("guest client follows sessions, includes archived threads from the complete reporter roster, and completes truncated transcript items", async () => {
  const calls = [];
  const fetcher = async (_url, request) => {
    const call = JSON.parse(request.body);
    calls.push(call);
    let result;
    if (call.method === "initialize") result = { protocolVersion: "2025-03-26" };
    else if (call.params.name === "t3_plugin_read")
      result = {
        threads: [
          { threadId: "first", subagent: false },
          { threadId: "archived-child", subagent: true },
        ],
      };
    else {
      const a = call.params.arguments;
      result = {
        items: [
          {
            itemId: "item",
            text: a.textOffset ? "tail" : "head",
            nextTextOffset: a.textOffset ? null : 4,
          },
        ],
        hasMore: false,
        nextPosition: null,
      };
    }
    return new Response(
      JSON.stringify({
        jsonrpc: "2.0",
        id: call.id,
        result: call.method === "initialize" ? result : { structuredContent: result },
      }),
      { headers: { "Content-Type": "application/json", "mcp-session-id": "session" } },
    );
  };
  const guest = new Guest("https://example.test", "secret", fetcher);
  assert.equal((await guest.threads()).length, 2);
  assert.equal(calls[1].params.arguments.commandId, "activity");
  assert.equal((await guest.threads())[1].relationshipToParent, "subagent");
  assert.equal((await guest.transcript("first")).items[0].text, "headtail");
});
