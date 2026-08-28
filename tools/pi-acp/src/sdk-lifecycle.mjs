export function isSuccessfulBuzzReplyCompletion(event) {
  return (
    event?.type === "tool_execution_end" &&
    event.toolName === "buzz_reply" &&
    event.isError === false
  );
}

export class TerminalPublicationLifecycle {
  #active = false;
  #terminal = false;

  beginPrompt() {
    this.#active = true;
    this.#terminal = false;
  }

  endPrompt() {
    this.#active = false;
  }

  acceptsSteering() {
    return this.#active && !this.#terminal;
  }

  settle(event, session, onTerminal) {
    if (!this.#active) return null;
    if (!isSuccessfulBuzzReplyCompletion(event)) return null;
    if (this.#terminal) return null;
    this.#active = false;
    this.#terminal = true;
    onTerminal?.();
    session.clearQueue();
    return session.abort();
  }
}
