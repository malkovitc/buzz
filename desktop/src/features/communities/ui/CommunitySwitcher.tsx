import {
  Check,
  ChevronDown,
  Link2,
  MoreHorizontal,
  Plus,
  Settings2,
  LogOut,
  Ticket,
  WifiOff,
} from "lucide-react";
import * as React from "react";
import { toast } from "sonner";

import type { LeaveCommunityResult } from "@/features/communities/leaveCommunity";
import type { Community } from "@/features/communities/types";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from "@/shared/ui/dropdown-menu";
import {
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/shared/ui/sidebar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/shared/ui/tooltip";
import { cn } from "@/shared/lib/cn";
import type { ConnectionState } from "@/shared/api/relayClientShared";
import {
  isRelayConnectionDegraded,
  useRelayConnection,
} from "@/shared/api/useRelayConnection";
import { writeTextToClipboard } from "@/shared/lib/clipboard";
import { useActiveCommunityIcon } from "@/features/communities/useCommunityIcons";
import { EditCommunityDialog } from "./EditCommunityDialog";

const CONNECTION_STATE_LABEL: Record<ConnectionState, string> = {
  idle: "Not connected",
  connecting: "Connecting…",
  connected: "Connected",
  reconnecting: "Reconnecting to relay…",
  stalled: "Connection lost — relay is not responding",
  disconnected: "Disconnected from relay",
};

type CommunitySwitcherProps = {
  activeCommunity: Community | null;
  communities: Community[];
  variant?: "sidebar" | "profile" | "profile-menu";
  canInvite?: boolean;
  onInvite?: () => void;
  onSwitchCommunity: (id: string) => void;
  onAddCommunity: () => void;
  onUpdateCommunity: (
    id: string,
    updates: Partial<Pick<Community, "name" | "relayUrl" | "token">>,
  ) => void;
  onRemoveCommunity: (id: string) => Promise<LeaveCommunityResult | undefined>;
};

export function CommunityEmojiIcon({
  className,
  iconUrl,
}: {
  className: string;
  iconUrl?: string | null;
}) {
  if (iconUrl) {
    return (
      <span
        aria-hidden="true"
        className={cn(className, "h-5 overflow-hidden rounded-md")}
      >
        <img
          alt=""
          className="h-full w-full object-cover"
          draggable={false}
          src={iconUrl}
        />
      </span>
    );
  }
  return (
    <span aria-hidden="true" className={className}>
      <span className="-translate-y-px leading-normal">🐝</span>
    </span>
  );
}

export function CommunitySwitcher({
  activeCommunity,
  communities,
  variant = "sidebar",
  canInvite = false,
  onInvite,
  onSwitchCommunity,
  onAddCommunity,
  onUpdateCommunity,
  onRemoveCommunity,
}: CommunitySwitcherProps) {
  const [editingCommunity, setEditingCommunity] =
    React.useState<Community | null>(null);
  const [dropdownOpen, setDropdownOpen] = React.useState(false);
  const [leaveError, setLeaveError] = React.useState<string | null>(null);
  const [isLeaving, setIsLeaving] = React.useState(false);
  const connectionState = useRelayConnection();
  const degraded = isRelayConnectionDegraded(connectionState);
  const connectionLabel = CONNECTION_STATE_LABEL[connectionState];
  const activeIconQuery = useActiveCommunityIcon(activeCommunity?.relayUrl);
  const activeIcon = activeIconQuery.data ?? null;
  const isProfileVariant = variant === "profile";

  const handleLeaveCommunity = React.useCallback(async () => {
    if (!activeCommunity || isLeaving) return;

    setIsLeaving(true);
    setLeaveError(null);
    try {
      const result = await onRemoveCommunity(activeCommunity.id);
      setDropdownOpen(false);
      if (result?.status === "already-absent") {
        toast("Community removed", {
          description:
            "You were no longer a member, so Buzz removed the community from this device.",
        });
      }
    } catch (error) {
      setLeaveError(
        error instanceof Error
          ? error.message
          : "Couldn't leave the community. Try again.",
      );
      setDropdownOpen(true);
    } finally {
      setIsLeaving(false);
    }
  }, [activeCommunity, isLeaving, onRemoveCommunity]);

  const triggerContent = (
    <>
      {degraded ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <span
              aria-hidden="false"
              className={
                isProfileVariant
                  ? "flex h-5 w-5 shrink-0 animate-pulse items-center justify-center rounded-md border border-sidebar-border/70 bg-sidebar-accent/40 text-destructive"
                  : "flex h-5 w-5 shrink-0 animate-pulse items-center justify-center text-destructive"
              }
              data-testid="relay-connection-warning"
              role="img"
            >
              <WifiOff className={isProfileVariant ? "h-4 w-4" : "h-4 w-4"} />
            </span>
          </TooltipTrigger>
          <TooltipContent side={isProfileVariant ? "top" : "bottom"}>
            {connectionLabel}
          </TooltipContent>
        </Tooltip>
      ) : (
        <CommunityEmojiIcon
          className={
            isProfileVariant
              ? "flex w-5 shrink-0 items-center justify-center rounded-md border border-sidebar-border/70 bg-sidebar-accent/40 text-2xs"
              : "flex w-5 shrink-0 items-center justify-center text-xs"
          }
          iconUrl={activeIcon}
        />
      )}
      <span
        className={
          degraded
            ? "min-w-0 flex-1 truncate font-medium text-destructive animate-pulse"
            : "min-w-0 flex-1 truncate font-medium"
        }
      >
        {activeCommunity?.name ?? "No community"}
      </span>
      {variant === "profile-menu" ? null : (
        <ChevronDown
          className={
            isProfileVariant
              ? "h-4 w-4 shrink-0 text-sidebar-foreground/45"
              : "h-4 w-4 shrink-0 text-sidebar-foreground/50"
          }
        />
      )}
    </>
  );

  const profileMenuSubmenu =
    variant === "profile-menu" ? (
      <DropdownMenuSub open={dropdownOpen} onOpenChange={setDropdownOpen}>
        <DropdownMenuSubTrigger
          aria-label={
            degraded
              ? `${activeCommunity?.name ?? "Community"} — ${connectionLabel}`
              : "Community actions"
          }
          className="px-3"
          data-testid="community-switcher"
        >
          {triggerContent}
        </DropdownMenuSubTrigger>
        <DropdownMenuSubContent
          align="end"
          aria-label="Community actions"
          className="w-60"
          data-testid="profile-community-actions"
          sideOffset={0}
        >
          {activeCommunity ? (
            <>
              <DropdownMenuItem
                onSelect={(event) => {
                  event.preventDefault();
                  setDropdownOpen(false);
                  void writeTextToClipboard(activeCommunity.relayUrl);
                }}
              >
                <Link2 className="h-4 w-4" />
                <span>Copy community URL</span>
              </DropdownMenuItem>
              {canInvite && onInvite ? (
                <DropdownMenuItem onSelect={onInvite}>
                  <Ticket className="h-4 w-4" />
                  <span>Invite to community</span>
                </DropdownMenuItem>
              ) : null}
              <DropdownMenuItem
                onSelect={() => setEditingCommunity(activeCommunity)}
              >
                <Settings2 className="h-4 w-4" />
                <span>Community settings</span>
              </DropdownMenuItem>
              <DropdownMenuItem
                className="text-destructive focus:bg-destructive/10 focus:text-destructive"
                disabled={isLeaving}
                onSelect={(event) => {
                  event.preventDefault();
                  void handleLeaveCommunity();
                }}
              >
                <LogOut className="h-4 w-4" />
                <span>{isLeaving ? "Leaving…" : "Leave community"}</span>
              </DropdownMenuItem>
              {leaveError ? (
                <p className="px-3 py-1 text-xs text-destructive" role="alert">
                  {leaveError}
                </p>
              ) : null}
              <DropdownMenuSeparator />
            </>
          ) : null}
          <DropdownMenuItem onSelect={onAddCommunity}>
            <Plus className="h-4 w-4" />
            <span>Add a community</span>
          </DropdownMenuItem>
        </DropdownMenuSubContent>
      </DropdownMenuSub>
    ) : null;

  const switcherDropdown = (
    <DropdownMenu
      modal={false}
      open={dropdownOpen}
      onOpenChange={setDropdownOpen}
    >
      <DropdownMenuTrigger asChild>
        {variant === "profile" ? (
          <button
            aria-label={
              degraded
                ? `${activeCommunity?.name ?? "Community"} — ${connectionLabel}`
                : "Switch community"
            }
            className="flex min-w-0 max-w-full items-center gap-1.5 rounded-md py-0.5 text-left text-xs text-sidebar-foreground/50 outline-hidden transition-colors hover:text-sidebar-foreground focus:outline-none focus-visible:outline-none data-[state=open]:text-sidebar-foreground"
            data-testid="community-switcher"
            type="button"
          >
            {triggerContent}
          </button>
        ) : (
          <SidebarMenuButton
            aria-label={
              degraded
                ? `${activeCommunity?.name ?? "Community"} — ${connectionLabel}`
                : undefined
            }
            className="h-auto gap-2 rounded-xl px-2.5 py-2 data-[state=open]:bg-sidebar-accent"
            data-testid="community-switcher"
            type="button"
          >
            {triggerContent}
          </SidebarMenuButton>
        )}
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="start"
        className="w-(--radix-dropdown-menu-trigger-width) min-w-[220px]"
        onCloseAutoFocus={(e) => e.preventDefault()}
        side={variant === "profile" ? "top" : "bottom"}
        sideOffset={4}
      >
        {communities.map((community) => (
          <DropdownMenuItem
            key={community.id}
            className="group flex items-center gap-2 pr-1"
            onSelect={() => {
              onSwitchCommunity(community.id);
            }}
          >
            <span className="flex h-4 w-4 shrink-0 items-center justify-center">
              {activeCommunity?.id === community.id ? (
                <Check className="h-4 w-4 text-primary" />
              ) : null}
            </span>
            <span className="min-w-0 flex-1 truncate">{community.name}</span>
            <button
              aria-label={`Edit ${community.name}`}
              className="flex h-5 w-5 shrink-0 items-center justify-center rounded opacity-0 hover:bg-accent group-hover:opacity-100 group-focus:opacity-100"
              onClick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                setDropdownOpen(false);
                setEditingCommunity(community);
              }}
              type="button"
            >
              <MoreHorizontal className="h-4 w-4" />
            </button>
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuItem onSelect={onAddCommunity}>
          <Plus className="h-4 w-4" />
          <span>Add a community</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );

  return (
    <>
      {variant === "profile" ? (
        switcherDropdown
      ) : variant === "profile-menu" ? (
        profileMenuSubmenu
      ) : (
        <SidebarMenu>
          <SidebarMenuItem>{switcherDropdown}</SidebarMenuItem>
        </SidebarMenu>
      )}

      <EditCommunityDialog
        onOpenChange={(open) => {
          if (!open) setEditingCommunity(null);
        }}
        onSave={onUpdateCommunity}
        open={editingCommunity !== null}
        community={editingCommunity}
        showIconEditor={editingCommunity?.id === activeCommunity?.id}
      />
    </>
  );
}
