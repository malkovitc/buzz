import * as React from "react";

import {
  ForumView,
  UserProfilePanel,
} from "@/features/channels/ui/ChannelScreenLazyViews";
import { AgentSessionThreadPanel } from "@/features/channels/ui/AgentSessionThreadPanel";
import * as agentSessionSelection from "@/features/channels/ui/agentSessionSelection";
import { RightAuxiliaryPane } from "@/features/channels/ui/RightAuxiliaryPane";
import type { ChannelAgentSessionAgent } from "@/features/channels/ui/useChannelAgentSessions";
import type {
  ProfilePanelTab,
  ProfilePanelView,
} from "@/features/profile/ui/UserProfilePanelUtils";
import type { UserProfileLookup } from "@/features/profile/lib/identity";
import type { Channel } from "@/shared/api/types";
import type { ProfilePanelOpenOptions } from "@/shared/context/ProfilePanelContext";
import { ViewLoadingFallback } from "@/shared/ui/ViewLoadingFallback";

type ForumChannelContentProps = {
  agentSessionAgents: ChannelAgentSessionAgent[];
  canResetPanelWidth: boolean;
  channel: Channel;
  currentPubkey?: string;
  header: React.ReactNode;
  onBackFromAgentSession?: () => void;
  onCloseAgentSession: () => void;
  onClosePost: () => void;
  onCloseProfilePanel: () => void;
  onOpenAgentSession: (pubkey: string, channelId?: string | null) => void;
  onOpenDm?: (pubkeys: string[]) => Promise<void> | void;
  onOpenProfilePanel: (
    pubkey: string,
    options?: ProfilePanelOpenOptions,
  ) => void;
  onPanelResizeStart: (event: React.PointerEvent<HTMLButtonElement>) => void;
  onProfilePanelTabChange: (
    tab: ProfilePanelTab,
    options?: { replace?: boolean },
  ) => void;
  onProfilePanelViewChange: (
    view: ProfilePanelView,
    options?: { replace?: boolean },
  ) => void;
  onResetPanelWidth: () => void;
  onSelectPost: (postId: string) => void;
  openAgentSessionPubkey: string | null;
  panelWidthPx: number;
  profilePanelPubkey?: string | null;
  profiles?: UserProfileLookup;
  profilePanelTab: ProfilePanelTab;
  profilePanelView: ProfilePanelView;
  selectedPostId: string | null;
  targetReplyId: string | null;
};

/**
 * Forum-channel body for ChannelScreen: the post list/thread plus the
 * user-profile auxiliary pane. Forums replace ChannelPane (which hosts the
 * profile panel for message channels), so without this host, opening a
 * profile from a mention chip, avatar, or the members sidebar would set
 * state that never renders.
 */
export function ForumChannelContent({
  agentSessionAgents,
  canResetPanelWidth,
  channel,
  currentPubkey,
  header,
  onBackFromAgentSession,
  onCloseAgentSession,
  onClosePost,
  onCloseProfilePanel,
  onOpenAgentSession,
  onOpenDm,
  onOpenProfilePanel,
  onPanelResizeStart,
  onProfilePanelTabChange,
  onProfilePanelViewChange,
  onResetPanelWidth,
  onSelectPost,
  openAgentSessionPubkey,
  panelWidthPx,
  profilePanelPubkey,
  profiles,
  profilePanelTab,
  profilePanelView,
  selectedPostId,
  targetReplyId,
}: ForumChannelContentProps) {
  const selectedAgent = React.useMemo(
    () =>
      agentSessionSelection.resolveSelectedAgentSession({
        agentSessionAgents,
        openAgentSessionPubkey,
        profilePanelPubkey,
        profiles,
      }),
    [agentSessionAgents, openAgentSessionPubkey, profilePanelPubkey, profiles],
  );

  return (
    <>
      {header}
      <div className="flex min-h-0 min-w-0 flex-1 flex-row overflow-hidden">
        <section
          aria-label="Forum posts"
          className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden"
        >
          <React.Suspense fallback={<ViewLoadingFallback kind="forum" />}>
            <ForumView
              channel={channel}
              currentPubkey={currentPubkey}
              onClosePost={onClosePost}
              onOpenAgentSession={onOpenAgentSession}
              openAgentSessionPubkey={openAgentSessionPubkey}
              onSelectPost={onSelectPost}
              selectedPostId={selectedPostId}
              targetReplyId={targetReplyId}
            />
          </React.Suspense>
        </section>
        {selectedAgent ? (
          <RightAuxiliaryPane
            canResetWidth={canResetPanelWidth}
            onResetWidth={onResetPanelWidth}
            onResizeStart={onPanelResizeStart}
            testId="agent-session-thread-panel"
            widthPx={panelWidthPx}
          >
            <AgentSessionThreadPanel
              agent={selectedAgent}
              canInterruptTurn={selectedAgent.canInterruptTurn}
              channel={channel}
              channelId={channel.id}
              layout="split"
              onBack={onBackFromAgentSession}
              onClose={onCloseAgentSession}
              profiles={profiles}
              transparentChrome
              widthPx={panelWidthPx}
            />
          </RightAuxiliaryPane>
        ) : profilePanelPubkey ? (
          <RightAuxiliaryPane
            canResetWidth={canResetPanelWidth}
            onResetWidth={onResetPanelWidth}
            onResizeStart={onPanelResizeStart}
            testId="user-profile-panel"
            widthPx={panelWidthPx}
          >
            <React.Suspense fallback={null}>
              <UserProfilePanel
                currentPubkey={currentPubkey}
                isSinglePanelView={false}
                layout="split"
                onClose={onCloseProfilePanel}
                onOpenDm={onOpenDm}
                onOpenProfile={onOpenProfilePanel}
                onTabChange={onProfilePanelTabChange}
                onViewChange={onProfilePanelViewChange}
                pubkey={profilePanelPubkey}
                splitPaneClamp
                tab={profilePanelTab}
                view={profilePanelView}
                widthPx={panelWidthPx}
              />
            </React.Suspense>
          </RightAuxiliaryPane>
        ) : null}
      </div>
    </>
  );
}
