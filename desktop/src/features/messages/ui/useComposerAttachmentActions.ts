import * as React from "react";

import { useAttachmentEditing } from "@/features/messages/lib/useAttachmentEditing";
import type { MediaUploadController } from "@/features/messages/lib/useMediaUpload";

const captureSelection = () => {};

export function useComposerAttachmentActions({
  media,
  setSpoileredUrls,
}: {
  media: MediaUploadController;
  setSpoileredUrls: React.Dispatch<React.SetStateAction<Set<string>>>;
}) {
  const onPaperclip = React.useCallback(() => {
    void media.handlePaperclip();
  }, [media.handlePaperclip]);
  const onRemove = React.useCallback(
    (url: string) => {
      setSpoileredUrls((current) => {
        if (!current.has(url)) return current;
        const next = new Set(current);
        next.delete(url);
        return next;
      });
      media.removeAttachment(url);
    },
    [media.removeAttachment, setSpoileredUrls],
  );
  const onToggleSpoiler = React.useCallback(
    (url: string) => {
      setSpoileredUrls((current) => {
        const next = new Set(current);
        next.has(url) ? next.delete(url) : next.add(url);
        return next;
      });
    },
    [setSpoileredUrls],
  );
  const { handleAttachmentEditSave, handleAttachmentRevert } =
    useAttachmentEditing({
      revertAttachment: media.revertAttachment,
      setSpoileredAttachmentUrls: setSpoileredUrls,
      uploadEditedAttachment: media.uploadEditedAttachment,
    });
  return {
    handleAttachmentEditSave,
    handleAttachmentRevert,
    onCaptureSelection: captureSelection,
    onPaperclip,
    onRemove,
    onToggleSpoiler,
  };
}
