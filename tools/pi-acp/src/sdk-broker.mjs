function abortError() {
  const error = new Error("brokered tool aborted");
  error.name = "AbortError";
  return error;
}

export class BrokerRequestRegistry {
  #nextId = 0;
  #pending = new Map();

  get size() {
    return this.#pending.size;
  }

  request(toolName, args, signal, send) {
    const id = `broker-${++this.#nextId}`;
    return new Promise((resolve, reject) => {
      const finish = (callback, value) => {
        const pending = this.#pending.get(id);
        if (!pending) return;
        this.#pending.delete(id);
        signal?.removeEventListener("abort", pending.abort);
        callback(value);
      };
      const abort = () => finish(reject, abortError());
      this.#pending.set(id, {
        abort,
        resolve: (value) => finish(resolve, value),
        reject: (error) => finish(reject, error),
      });
      if (signal?.aborted) {
        abort();
        return;
      }
      signal?.addEventListener("abort", abort, { once: true });
      try {
        send({ type: "broker_tool_request", id, toolName, args });
      } catch (error) {
        this.#pending.get(id)?.reject(error);
      }
    });
  }

  respond(message) {
    const pending = this.#pending.get(message.id);
    if (!pending) return false;
    if (message.success) pending.resolve(message.result);
    else pending.reject(new Error(message.error || "brokered tool failed"));
    return true;
  }

  rejectAll(error = new Error("broker bridge closed")) {
    for (const pending of [...this.#pending.values()]) pending.reject(error);
  }
}
