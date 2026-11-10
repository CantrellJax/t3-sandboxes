export class Guest {
  constructor(url, token, fetcher = fetch) {
    this.url = new URL("/mcp", url);
    this.token = token;
    this.fetcher = fetcher;
    this.id = 0;
  }
  async rpc(method, params) {
    const response = await this.fetcher(this.url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
        Accept: "application/json, text/event-stream",
        ...(this.session ? { "Mcp-Session-Id": this.session } : {}),
      },
      body: JSON.stringify({ jsonrpc: "2.0", id: ++this.id, method, params }),
      signal: AbortSignal.timeout(30000),
    });
    if (!response.ok) throw new Error(`Guest request failed (${response.status})`);
    this.session = response.headers.get("mcp-session-id") ?? this.session;
    const text = await response.text();
    if (text.length > 8 * 1024 * 1024) throw new Error("Guest response too large");
    const messages = response.headers.get("content-type")?.includes("text/event-stream")
      ? text
          .split("\n")
          .filter((x) => x.startsWith("data:"))
          .map((x) => JSON.parse(x.slice(5)))
      : [JSON.parse(text)];
    const message = messages.find((x) => x.id === this.id);
    if (!message || message.error) throw new Error("Guest RPC failed");
    return message.result;
  }
  async call(name, args = {}) {
    if (!this.initialized) {
      await this.rpc("initialize", {
        protocolVersion: "2025-03-26",
        capabilities: {},
        clientInfo: { name: "t3-sandboxes", version: "0.1.0" },
      });
      this.initialized = true;
    }
    const result = await this.rpc("tools/call", { name, arguments: args });
    if (result.isError) throw new Error(`Guest tool failed: ${name}`);
    return (
      result.structuredContent ?? JSON.parse(result.content.find((x) => x.type === "text").text)
    );
  }
  async threads() {
    const all = [];
    let cursor;
    do {
      const page = await this.call("t3_thread_list", {
        limit: 100,
        allProjects: true,
        includeSubagents: true,
        ...(cursor === undefined ? {} : { cursor }),
      });
      all.push(...page.threads);
      cursor = page.nextCursor;
    } while (cursor !== null && cursor !== undefined);
    return all;
  }
  async activity() {
    return this.call("t3_plugin_read", {
      pluginId: "sandboxes",
      commandId: "activity",
      input: {},
      permission: "orchestration:read",
    });
  }
  async transcript(threadId) {
    const items = [];
    let afterPosition;
    do {
      const page = await this.call("t3_thread_read", {
        threadId,
        view: "activity",
        limit: 50,
        maxCharsPerItem: 50000,
        ...(afterPosition === undefined ? {} : { afterPosition }),
      });
      for (const item of page.items) {
        let full = item;
        while (full.nextTextOffset !== null && full.nextTextOffset !== undefined) {
          const extra = await this.call("t3_thread_read", {
            threadId,
            itemId: item.itemId,
            textOffset: full.nextTextOffset,
            maxCharsPerItem: 50000,
          });
          const part = extra.items[0];
          full = {
            ...full,
            text: (full.text ?? "") + (part.text ?? ""),
            nextTextOffset: part.nextTextOffset,
          };
        }
        items.push(full);
      }
      afterPosition = page.hasMore ? page.nextPosition : null;
    } while (afterPosition !== null);
    return { threadId, items };
  }
}
