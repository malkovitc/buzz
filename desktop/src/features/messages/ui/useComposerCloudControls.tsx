import * as React from "react";

import {
  cloudControlSuggestions,
  cloudControlWireCommand,
  EMPTY_CLOUD_CONTROL_COMMANDS,
  type CloudControlCommand,
} from "@/features/messages/lib/cloudControlCommands";
import type { UseRichTextEditorResult } from "@/features/messages/lib/useRichTextEditor";
import type { MessageComposerCloudControls } from "./MessageComposer.types";
import { CloudControlAutocomplete } from "./CloudControlAutocomplete";
import { CloudControlMenu } from "./CloudControlMenu";

export function useComposerCloudControls({
  config,
  contentEmpty,
  disabled,
  editing,
  mediaBlocked,
  richText,
  toolbarExtraActions,
}: {
  config?: MessageComposerCloudControls;
  contentEmpty: boolean;
  disabled: boolean;
  editing: boolean;
  mediaBlocked: boolean;
  richText: Pick<
    UseRichTextEditorResult,
    "focusEnd" | "getPlainTextAndCursor" | "setContent"
  >;
  toolbarExtraActions?: React.ReactNode;
}) {
  const [matches, setMatches] = React.useState<readonly CloudControlCommand[]>(
    EMPTY_CLOUD_CONTROL_COMMANDS,
  );
  const [selectedIndex, setSelectedIndex] = React.useState(0);
  const [draftCommand, setDraftCommand] =
    React.useState<CloudControlCommand | null>(null);
  const enabled = Boolean(config && !editing);
  const transitionPending = config?.state.startsWith("switching") ?? false;

  const reset = React.useCallback(() => {
    setMatches(EMPTY_CLOUD_CONTROL_COMMANDS);
    setSelectedIndex(0);
    setDraftCommand(null);
  }, []);
  const onEditorUpdate = React.useCallback(
    (text: string, cursor: number) => {
      const suggestions =
        enabled && !mediaBlocked
          ? cloudControlSuggestions(text, cursor)
          : EMPTY_CLOUD_CONTROL_COMMANDS;
      const next = transitionPending
        ? suggestions.filter((command) => command.id === "status")
        : suggestions;
      setMatches(next);
      setSelectedIndex((current) =>
        Math.min(current, Math.max(0, next.length - 1)),
      );
      setDraftCommand(enabled ? cloudControlWireCommand(text) : null);
    },
    [enabled, mediaBlocked, transitionPending],
  );
  const select = React.useCallback(
    (command: CloudControlCommand) => {
      richText.setContent(command.slash);
      richText.focusEnd();
      setMatches(EMPTY_CLOUD_CONTROL_COMMANDS);
      setSelectedIndex(0);
      setDraftCommand(command);
    },
    [richText.focusEnd, richText.setContent],
  );
  const handleKeyDown = React.useCallback(
    (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (matches.length === 0) return false;
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        const direction = event.key === "ArrowDown" ? 1 : -1;
        setSelectedIndex(
          (current) => (current + direction + matches.length) % matches.length,
        );
        return true;
      }
      if (event.key === "Enter" || event.key === "Tab") {
        event.preventDefault();
        const command = matches[selectedIndex];
        if (command) select(command);
        return true;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        setMatches(EMPTY_CLOUD_CONTROL_COMMANDS);
        return true;
      }
      return false;
    },
    [matches, select, selectedIndex],
  );
  const autocomplete = (
    <CloudControlAutocomplete
      onSelect={select}
      selectedIndex={selectedIndex}
      suggestions={matches}
    />
  );
  const toolbarActions = React.useMemo(() => {
    if (!enabled || !config) return toolbarExtraActions;
    return (
      <>
        {toolbarExtraActions}
        <CloudControlMenu
          disabled={disabled || mediaBlocked || !contentEmpty}
          onSelect={select}
          state={config.state}
          transitionPending={transitionPending}
        />
      </>
    );
  }, [
    config,
    contentEmpty,
    disabled,
    enabled,
    mediaBlocked,
    select,
    toolbarExtraActions,
    transitionPending,
  ]);
  const isCommandBlocked = React.useCallback(
    (command: CloudControlCommand) =>
      transitionPending && command.id !== "status",
    [transitionPending],
  );

  return {
    autocomplete,
    commandFromText: enabled ? cloudControlWireCommand : () => null,
    handleKeyDown,
    isAutocompleteOpen: matches.length > 0,
    isCommandBlocked,
    isCommandDraft: draftCommand !== null,
    isSendBlocked: draftCommand ? isCommandBlocked(draftCommand) : false,
    onEditorUpdate,
    reset,
    toolbarActions,
  };
}
