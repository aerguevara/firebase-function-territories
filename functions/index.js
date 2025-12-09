const { onDocumentWritten } = require('firebase-functions/v2/firestore');
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
