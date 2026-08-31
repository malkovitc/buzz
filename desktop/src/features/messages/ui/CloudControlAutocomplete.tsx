import * as React from "react";

import type { CloudControlCommand } from "@/features/messages/lib/cloudControlCommands";
import { cn } from "@/shared/lib/cn";
import {
  POPOVER_CUSTOM_ENTER_MOTION_CLASS,
  POPOVER_SHADOW_STYLE,
  POPOVER_SURFACE_CLASS,
} from "@/shared/ui/popoverSurface";

export const CloudControlAutocomplete = React.memo(
  function CloudControlAutocomplete({
    suggestions,
    selectedIndex,
    onSelect,
  }: {
    suggestions: readonly CloudControlCommand[];
    selectedIndex: number;
    onSelect: (command: CloudControlCommand) => void;
  }) {
    if (suggestions.length === 0) return null;
    return (
      <div className="absolute bottom-full left-0 right-0 z-50 mb-1 px-3 sm:px-4">
        <div
          className={cn(
            "max-h-48 overflow-y-auto rounded-xl p-1 origin-bottom slide-in-from-bottom-1",
            POPOVER_CUSTOM_ENTER_MOTION_CLASS,
            POPOVER_SURFACE_CLASS,
          )}
          style={POPOVER_SHADOW_STYLE}
        >
          {suggestions.map((command, index) => (
            <button
              className={cn(
                "flex w-full items-center gap-3 rounded-lg px-3 py-2 text-left text-sm",
                index === selectedIndex
                  ? "bg-accent text-accent-foreground"
                  : "text-popover-foreground hover:bg-accent/50",
              )}
              key={command.id}
              onMouseDown={(event) => {
                event.preventDefault();
                onSelect(command);
              }}
              tabIndex={-1}
              type="button"
            >
              <code className="shrink-0 font-medium">{command.slash}</code>
              <span className="min-w-0">
                <span className="block font-medium">{command.label}</span>
                <span className="block truncate text-xs text-muted-foreground">
                  {command.description}
                </span>
              </span>
            </button>
          ))}
        </div>
      </div>
    );
  },
);
