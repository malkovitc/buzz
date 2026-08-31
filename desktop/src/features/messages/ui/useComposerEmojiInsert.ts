import * as React from "react";
import type { Editor } from "@tiptap/react";

import { CUSTOM_EMOJI_NODE_NAME } from "@/features/messages/lib/customEmojiNode";
import type { CustomEmoji } from "@/shared/lib/remarkCustomEmoji";

export function useComposerEmojiInsert({
  clearMentions,
  customEmoji,
  editor,
  setPickerOpen,
}: {
  clearMentions: () => void;
  customEmoji: readonly CustomEmoji[];
  editor: Editor | null;
  setPickerOpen: React.Dispatch<React.SetStateAction<boolean>>;
}) {
  return React.useCallback(
    (emoji: string) => {
      if (!editor) return;
      const shortcode = /^:([^:\s]+):$/.exec(emoji)?.[1]?.toLowerCase();
      const known =
        shortcode &&
        customEmoji.some((item) => item.shortcode.toLowerCase() === shortcode);
      if (known && shortcode) {
        editor
          .chain()
          .focus()
          .insertContent({
            type: CUSTOM_EMOJI_NODE_NAME,
            attrs: {
              shortcode,
              src:
                customEmoji.find(
                  (item) => item.shortcode.toLowerCase() === shortcode,
                )?.url ?? "",
            },
          })
          .insertContent(" ")
          .run();
      } else {
        editor.chain().focus().insertContent(emoji).run();
      }
      setPickerOpen(false);
      clearMentions();
    },
    [clearMentions, customEmoji, editor, setPickerOpen],
  );
}
