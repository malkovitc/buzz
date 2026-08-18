import { Terminal } from "lucide-react";

import { ScrollFadeMonoPanel } from "../FileContentBlock";
import { parseShellToolOutput } from "../agentSessionUtils";

export function ShellCommandBlock({
  command,
  result,
}: {
  command: string;
  result: string;
}) {
  const output = parseShellToolOutput(result);
  const stdout = (output.stdout || output.raw).trimEnd();
  const stderr = output.stderr.trimEnd();

  return (
    <div
      className="overflow-hidden rounded-lg bg-muted font-mono text-xs leading-5"
      data-testid="transcript-shell-command"
    >
      <ScrollFadeMonoPanel
        fadeFromClassName="from-muted"
        maxHeightClassName="max-h-36"
      >
        <p className="whitespace-pre-wrap wrap-break-word text-muted-foreground/70">
          <Terminal className="mr-2 inline h-3.5 w-3.5 align-[-0.1875rem] text-primary" />
          {command}
        </p>
      </ScrollFadeMonoPanel>
      {stdout ? (
        <ScrollFadeMonoPanel
          className="mt-2"
          fadeFromClassName="from-muted"
          maxHeightClassName="max-h-36"
        >
          <pre className="whitespace-pre-wrap wrap-break-word text-foreground">
            {stdout}
          </pre>
        </ScrollFadeMonoPanel>
      ) : null}
      {stderr ? (
        <ScrollFadeMonoPanel
          className="mt-2 border-t border-destructive/20 pt-2"
          fadeFromClassName="from-muted"
          maxHeightClassName="max-h-36"
        >
          <p className="mb-1 text-2xs font-semibold uppercase tracking-wide text-destructive/80">
            stderr
          </p>
          <pre className="whitespace-pre-wrap wrap-break-word text-destructive">
            {stderr}
          </pre>
        </ScrollFadeMonoPanel>
      ) : null}
      {output.exitCode !== null || output.timedOut ? (
        <p className="mt-2 text-2xs text-muted-foreground">
          {output.timedOut ? "Timed out" : `Exited ${output.exitCode}`}
        </p>
      ) : null}
    </div>
  );
}
