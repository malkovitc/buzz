import * as React from "react";
import {
  ChevronDown,
  Cloud,
  HardDrive,
  LoaderCircle,
  ShieldAlert,
} from "lucide-react";

import {
  CLOUD_CONTROL_COMMANDS,
  type CloudControlCommand,
  type CloudControlUiState,
} from "@/features/messages/lib/cloudControlCommands";
import { cn } from "@/shared/lib/cn";
import { Button } from "@/shared/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/shared/ui/dropdown-menu";

const STATE_PRESENTATION = {
  unknown: { label: "Location", icon: HardDrive },
  local: { label: "Local", icon: HardDrive },
  cloud: { label: "Cloud", icon: Cloud },
  "switching-local": { label: "Returning…", icon: LoaderCircle },
  "switching-cloud": { label: "Moving…", icon: LoaderCircle },
  blocked: { label: "Blocked", icon: ShieldAlert },
} satisfies Record<CloudControlUiState, { label: string; icon: typeof Cloud }>;

export function CloudControlMenu({
  disabled,
  state,
  onSelect,
  transitionPending,
}: {
  disabled: boolean;
  state: CloudControlUiState;
  onSelect: (command: CloudControlCommand) => void;
  transitionPending: boolean;
}) {
  const presentation = STATE_PRESENTATION[state];
  const StateIcon = presentation.icon;
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          aria-label={`Agent location: ${presentation.label}`}
          className="h-8 gap-1.5 rounded-full px-2.5 text-xs"
          disabled={disabled}
          size="sm"
          type="button"
          variant="outline"
        >
          <StateIcon
            aria-hidden
            className={cn(
              "size-3.5",
              state.startsWith("switching") && "animate-spin",
            )}
          />
          {presentation.label}
          <ChevronDown aria-hidden className="size-3" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-64">
        {CLOUD_CONTROL_COMMANDS.map((command, index) => (
          <React.Fragment key={command.id}>
            {index === 2 ? <DropdownMenuSeparator /> : null}
            <DropdownMenuItem
              disabled={transitionPending && command.id !== "status"}
              onSelect={() => onSelect(command)}
            >
              <span className="min-w-0">
                <span className="block font-medium">{command.label}</span>
                <span className="block truncate text-xs text-muted-foreground">
                  {command.slash} · {command.description}
                </span>
              </span>
            </DropdownMenuItem>
          </React.Fragment>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
