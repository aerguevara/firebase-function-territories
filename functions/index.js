const { onDocumentWritten, onDocumentCreated } = require('firebase-functions/v2/firestore');
const logger = require('firebase-functions/logger');
const admin = require('firebase-admin');

admin.initializeApp();

const { FieldValue } = admin.firestore;

/**
 * Track ownership and activity changes for remote_territories documents.
 * When userId or activityId changes (create or update), write a record to the "owners"
 * subcollection with previous and new values plus timing metadata.
 */
exports.remoteTerritoryOwnerHistory = onDocumentWritten(
  'remote_territories/{territoryId}',
  async (event) => {
    const territoryId = event.params.territoryId;
    const afterSnapshot = event.data.after;
    const beforeSnapshot = event.data.before;
    const eventId = event.id;

    // Skip deletes: keep history only while the territory exists.
    if (!afterSnapshot.exists) {
      return null;
    }

    const afterData = afterSnapshot.data() || {};
    const beforeData = beforeSnapshot.data() || {};

    const previousUserId = beforeSnapshot.exists ? beforeData.userId || null : null;
    const newUserId = afterData.userId || null;
    const previousActivityId = beforeSnapshot.exists ? beforeData.activityId || null : null;
    const newActivityId = afterData.activityId || null;
    const isCreate = !beforeSnapshot.exists;
    const ownerChanged = isCreate ? newUserId !== null : previousUserId !== newUserId;
    const activityChanged = isCreate ? newActivityId !== null : previousActivityId !== newActivityId;

    // Nothing to do if neither owner nor activity changed.
    if (!ownerChanged && !activityChanged) {
      return null;
    }

    const ownersCollection = afterSnapshot.ref.collection('owners');

    const historyDocId = eventId; // makes the write idempotent if the event is retried

    await ownersCollection.doc(historyDocId).set({
      eventId,
      territoryId,
      previousUserId,
      newUserId,
      previousActivityId,
      newActivityId,
      changedAt: FieldValue.serverTimestamp(),
      expiresAt: afterData.expiresAt || null,
      activityEndAt: afterData.activityEndAt || afterData.timestamp || null,
      changeType: isCreate ? 'create' : 'update'
    });

    return null;
  }
);

/**
 * Send a push notification when a new feed item is created.
 * Feed shape (example):
 * {
 *   id: "activity-UUID-social",
 *   type: "weeklySummary",
 *   date: "2024-06-20T14:32:10Z",
 *   activityId: "...",
 *   title: "Activity Completed",
 *   subtitle: "Optional detail",
 *   xpEarned: 120,
 *   userId: "<author>",
 *   relatedUserName: "...",
 *   userAvatarURL: "https://.../avatar.jpg",
 *   isPersonal: false,
 *   activityData: {...}
 * }
 *
 * Sends to all users except the author (userId) by collecting their FCM tokens
 * from users/{uid} (fcmTokens array, fcmToken, or tokens).
 */
exports.feedPushNotification = onDocumentCreated(
  'feed/{feedId}',
  async (event) => {
    const feedId = event.params.feedId;
    const feedData = event.data.data() || {};
    const userId = feedData.userId;

    if (!userId) {
      logger.warn('feed doc missing userId; skipping push', { feedId });
      return null;
    }

    const title = feedData.title || 'Nuevo evento';
    const body =
      feedData.subtitle ||
      feedData.body ||
      feedData.message ||
      (feedData.xpEarned != null ? `Ganaste ${feedData.xpEarned} XP` : '') ||
      '';

    const db = admin.firestore();
    const usersSnapshot = await db.collection('users').get();

    // Collect tokens for all users except the author.
    const tokens = [];
    const tokenOwners = new Map(); // token -> Set<docRef> to prune invalids

    usersSnapshot.forEach((doc) => {
      if (doc.id === userId) {
        return;
      }
      const data = doc.data() || {};
      let userTokens = [];
      if (Array.isArray(data.fcmTokens)) {
        userTokens = userTokens.concat(data.fcmTokens);
      }
      if (data.fcmToken) {
        userTokens.push(data.fcmToken);
      }
      if (Array.isArray(data.tokens)) {
        userTokens = userTokens.concat(data.tokens);
      }
      userTokens = Array.from(new Set(userTokens.filter(Boolean)));
      if (!userTokens.length) {
        return;
      }
      userTokens.forEach((t) => {
        tokens.push(t);
        if (!tokenOwners.has(t)) {
          tokenOwners.set(t, new Set());
        }
        tokenOwners.get(t).add(doc.ref);
      });
    });

    const uniqueTokens = Array.from(new Set(tokens));

    if (!uniqueTokens.length) {
      logger.info('No FCM tokens for audience; skipping push', { feedId, authorId: userId });
      return null;
    }

    const chunks = [];
    const chunkSize = 500; // FCM multicast limit
    for (let i = 0; i < uniqueTokens.length; i += chunkSize) {
      chunks.push(uniqueTokens.slice(i, i + chunkSize));
    }

    let totalSuccess = 0;
    let totalFailure = 0;
    const invalidTokens = new Set();

    for (const tokenChunk of chunks) {
      const message = {
        tokens: tokenChunk,
        notification: {
          title,
          body,
          image: feedData.userAvatarURL || undefined
        },
        data: {
          feedId,
          userId,
          activityId: (feedData.activityId || '').toString(),
          type: (feedData.type || '').toString(),
          date: (feedData.date || '').toString(),
          isPersonal: (feedData.isPersonal ?? '').toString()
        }
      };

      const response = await admin.messaging().sendEachForMulticast(message);
      totalSuccess += response.successCount;
      totalFailure += response.failureCount;

      response.responses.forEach((resp, idx) => {
        if (!resp.success) {
          const code = resp.error?.code || '';
          if (
            code === 'messaging/registration-token-not-registered' ||
            code === 'messaging/invalid-registration-token'
          ) {
            invalidTokens.add(tokenChunk[idx]);
          }
        }
      });
    }

    if (invalidTokens.size) {
      const removals = new Map(); // ref.path -> { ref, tokens[] }
      invalidTokens.forEach((token) => {
        const owners = tokenOwners.get(token);
        if (!owners) return;
        owners.forEach((ref) => {
          const key = ref.path;
          if (!removals.has(key)) {
            removals.set(key, { ref, tokens: [] });
          }
          removals.get(key).tokens.push(token);
        });
      });

      await Promise.all(
        Array.from(removals.values()).map(({ ref, tokens: toks }) =>
          ref.update({
            fcmTokens: FieldValue.arrayRemove(...toks),
            tokens: FieldValue.arrayRemove(...toks)
          }).catch((err) => {
            logger.warn('Failed to prune invalid tokens', { feedId, authorId: userId, error: err });
          })
        )
      );
    }

    logger.info('feed push processed', {
      feedId,
      authorId: userId,
      audienceTokens: uniqueTokens.length,
      successCount: totalSuccess,
      failureCount: totalFailure
    });

    return null;
  }
);
