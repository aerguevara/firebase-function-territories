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
  { region: 'us-central1', retry: true, document: 'feed/{feedId}' },
  async (event) => {
    const eventId = event.id;
    const feedSnapshot = event.data;
    if (!feedSnapshot) {
      logger.error('feed event missing data', { eventId });
      return null;
    }
    const feedData = feedSnapshot.data() || {};
    const feedId = (event.params && event.params.feedId) || feedData.id || 'unknown';
    const feedRef = feedSnapshot.ref;
    const userId = feedData.userId;
    const feedTokens = Array.isArray(feedData.tokens)
      ? feedData.tokens.filter(Boolean)
      : feedData.token
        ? [feedData.token]
        : [];

    // Dedup if we already marked this feed as sent.
    if (feedData.pushSentAt) {
      logger.info('feed push already marked sent; skipping', { feedId, eventId });
      return null;
    }

    // If no author and no explicit tokens, skip and annotate.
    if (!userId && feedTokens.length === 0) {
      logger.warn('feed doc missing userId and tokens; skipping push', { feedId, eventId });
      await feedRef.set(
        {
          pushStatus: 'skipped',
          pushError: 'missing userId and tokens',
          pushEventId: eventId
        },
        { merge: true }
      );
      return null;
    }

    const isPersonal = !!feedData.isPersonal;
    const authorName = feedData.relatedUserName || 'Un jugador';
    const activityType = (feedData.activityData && feedData.activityData.activityType) || feedData.type || '';
    const xp = feedData.activityData?.xpEarned ?? feedData.xpEarned;
    const distanceMeters = feedData.activityData?.distanceMeters;
    const distanceText =
      typeof distanceMeters === 'number' && !Number.isNaN(distanceMeters)
        ? `${(distanceMeters / 1000).toFixed(1)} km`
        : '';

    const title = isPersonal
      ? feedData.title || 'Tu actividad se completó'
      : feedData.title || `${authorName} completó una actividad`;

    const body = isPersonal
      ? feedData.subtitle ||
        feedData.body ||
        feedData.message ||
        (xp != null ? `Ganaste ${xp} XP` : '') ||
        ''
      : feedData.subtitle ||
        feedData.body ||
        feedData.message ||
        (distanceText
          ? `${authorName} completó ${distanceText}${activityType ? ` (${activityType})` : ''}`
          : xp != null
            ? `${authorName} ganó ${xp} XP`
            : `${authorName} completó una actividad`);

    const db = admin.firestore();

    try {
      const tokens = [...feedTokens];
      const tokenOwners = new Map(); // token -> Set<docRef> to prune invalids

      if (userId) {
        const usersSnapshot = await db.collection('users').get();

        // Collect tokens for all users except the author.
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
      }

      const uniqueTokens = Array.from(new Set(tokens.filter(Boolean)));

      if (!uniqueTokens.length) {
        logger.info('No FCM tokens for audience; skipping push', { feedId, authorId: userId, eventId });
        await feedRef.set(
          {
            pushStatus: 'skipped',
            pushError: 'no audience tokens',
            pushEventId: eventId
          },
          { merge: true }
        );
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
      const failureReasons = {};
      const refreshUsers = new Set(); // users that need to refresh token (e.g., third-party-auth-error)

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
            userId: userId || '',
            activityId: (feedData.activityId || '').toString(),
            type: (feedData.type || '').toString(),
            date: (feedData.date || '').toString(),
            isPersonal: (feedData.isPersonal ?? '').toString(),
            relatedUserName: authorName,
            eventId
          }
        };

        try {
          const response = await admin.messaging().sendEachForMulticast(message);
          totalSuccess += response.successCount;
          totalFailure += response.failureCount;

          response.responses.forEach((resp, idx) => {
            if (!resp.success) {
              const code = resp.error?.code || '';
              failureReasons[code || 'unknown'] = (failureReasons[code || 'unknown'] || 0) + 1;
              if (
                code === 'messaging/registration-token-not-registered' ||
                code === 'messaging/invalid-registration-token' ||
                code === 'messaging/third-party-auth-error'
              ) {
                invalidTokens.add(tokenChunk[idx]);
                if (code === 'messaging/third-party-auth-error') {
                  const owners = tokenOwners.get(tokenChunk[idx]);
                  if (owners) {
                    owners.forEach((ref) => refreshUsers.add(ref));
                  }
                }
              }
            }
          });
        } catch (sendErr) {
          logger.error('push send chunk failed', { feedId, eventId, error: sendErr });
          totalFailure += tokenChunk.length;
          const code = sendErr.code || sendErr.message || 'chunk_error';
          failureReasons[code] = (failureReasons[code] || 0) + tokenChunk.length;
        }
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
              logger.warn('Failed to prune invalid tokens', { feedId, authorId: userId, eventId, error: err });
            })
          )
        );
      }

      if (refreshUsers.size) {
        await Promise.all(
          Array.from(refreshUsers).map((ref) =>
            ref.update({
              needsTokenRefresh: true,
              needsTokenRefreshAt: FieldValue.serverTimestamp()
            }).catch((err) => {
              logger.warn('Failed to mark user for token refresh', { userRef: ref.path, feedId, eventId, error: err });
            })
          )
        );
      }

      await feedRef.set(
        {
          pushStatus: totalSuccess > 0 ? 'sent' : 'failed',
          pushSentAt: FieldValue.serverTimestamp(),
          pushEventId: eventId,
          pushSuccessCount: totalSuccess,
          pushFailureCount: totalFailure,
          pushAudienceTokens: uniqueTokens.length,
          pushFailureReasons: Object.keys(failureReasons).length ? failureReasons : FieldValue.delete(),
          pushError: totalSuccess > 0 ? FieldValue.delete() : (Object.keys(failureReasons)[0] || 'all failed')
        },
        { merge: true }
      );

      logger.info('feed push processed', {
        feedId,
        authorId: userId,
        eventId,
        audienceTokens: uniqueTokens.length,
        successCount: totalSuccess,
        failureCount: totalFailure
      });

      return null;
    } catch (err) {
      logger.error('feed push failed', { feedId, authorId: userId, eventId, error: err });
      await feedRef.set(
        {
          pushStatus: 'failed',
          pushError: (err && err.message) || 'unknown error',
          pushEventId: eventId
        },
        { merge: true }
      ).catch(() => {});
      return null;
    }
  }
);
