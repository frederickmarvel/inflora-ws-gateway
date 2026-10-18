package seq

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Frame struct {
	Type       string          `json:"type"`
	Seq        int64           `json:"seq"`
	StreamerID string          `json:"streamer_id"`
	TS         time.Time       `json:"ts"`
	Data       json.RawMessage `json:"data,omitempty"`
}
type Store struct {
	DB        *sql.DB
	Retention time.Duration

	// D-038 schema readiness: Project's reject guard needs donations.
	// review_status (migration 00011). Probed once, lazily, so a mis-ordered
	// deploy (gateway before migration) degrades to the pre-D-038 unguarded
	// behavior instead of failing every charged projection.
	reviewProbeOnce sync.Once
	reviewReady     atomic.Bool
}

func (s *Store) Project(ctx context.Context, consumer, eventID, eventType, streamer string, data json.RawMessage) (Frame, bool, error) {
	var out Frame
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback()
	var exists bool
	if e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ws_projection_inbox WHERE consumer_name=$1 AND event_id=$2)`, consumer, eventID).Scan(&exists); e != nil {
		return out, false, e
	}
	if exists {
		return out, true, tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO ws_projection_inbox(consumer_name,event_id,event_type) VALUES($1,$2,$3)`, consumer, eventID, eventType); err != nil {
		return out, false, err
	}
	if skipped, err := s.skipRejectedCharge(ctx, tx, eventType, data); err != nil {
		return out, false, err
	} else if skipped {
		return out, false, tx.Commit()
	}
	var n int64
	if err = tx.QueryRowContext(ctx, `INSERT INTO ws_projection_sequences(streamer_id,next_seq) VALUES($1,2) ON CONFLICT(streamer_id) DO UPDATE SET next_seq=ws_projection_sequences.next_seq+1,updated_at=NOW() RETURNING next_seq-1`, streamer).Scan(&n); err != nil {
		return out, false, err
	}
	out = Frame{Type: "donation", Seq: n, StreamerID: streamer, TS: time.Now().UTC(), Data: data}
	if eventType == "donation.settled.v1" {
		out.Type = "donation_settled"
	}
	payload, _ := json.Marshal(out)
	var expiry any
	if s.Retention > 0 {
		expiry = time.Now().Add(s.Retention)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO ws_projection_frames(streamer_id,seq,source_event_id,event_type,payload,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, streamer, n, eventID, eventType, payload, expiry); err != nil {
		return out, false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE ws_projection_inbox SET processed_at=NOW() WHERE consumer_name=$1 AND event_id=$2`, consumer, eventID); err != nil {
		return out, false, err
	}
	return out, false, tx.Commit()
}

	// Remove projects a donation TAKEDOWN (CONTRACT_DRIFT D-038): the projection
// row for the donation is deleted so replays can never re-air it, and a
// removal frame is returned for live broadcast to connected overlays. The
// removal frame is broadcast-only — it is never persisted or replayed, because
// a deleted row needs no removal on replay.
func (s *Store) Remove(ctx context.Context, consumer, eventID, eventType, streamer, donationID string) (Frame, bool, error) {
	var out Frame
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback()
	var exists bool
	if e := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ws_projection_inbox WHERE consumer_name=$1 AND event_id=$2)`, consumer, eventID).Scan(&exists); e != nil {
		return out, false, e
	}
	if exists {
		// D-038 crash gap: the inbox row is committed before the live overlay
		// broadcast, so a crash in between (or an unacked delivery) must
		// re-broadcast the removal frame on the next delivery. The frame is
		// ephemeral and the overlay removal is idempotent, so re-sending it
		// on every redelivery is safe.
		return removalFrame(streamer, donationID), true, tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO ws_projection_inbox(consumer_name,event_id,event_type) VALUES($1,$2,$3)`, consumer, eventID, eventType); err != nil {
		return out, false, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM ws_projection_frames WHERE streamer_id=$1 AND payload->'data'->>'donation_id'=$2`, streamer, donationID)
	if err != nil {
		return out, false, err
	}
	n, _ := res.RowsAffected()
	if _, err = tx.ExecContext(ctx, `UPDATE ws_projection_inbox SET processed_at=NOW() WHERE consumer_name=$1 AND event_id=$2`, consumer, eventID); err != nil {
		return out, false, err
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}
	if n == 0 {
		// The donation never projected (or its frame was already removed):
		// nothing to broadcast, and the inbox keeps the event consumed.
		return out, false, nil
	}
	return removalFrame(streamer, donationID), true, nil
}

// removalFrame builds the ephemeral broadcast-only takedown frame
// (CONTRACT_DRIFT D-038): never persisted, never replayed, seq 0.
func removalFrame(streamer, donationID string) Frame {
	data, _ := json.Marshal(map[string]string{"donation_id": donationID})
	return Frame{Type: "donation_removed", Seq: 0, StreamerID: streamer, TS: time.Now().UTC(), Data: data}
}

// reviewSchemaReady lazily probes for donations.review_status (migration
// 00011). Before the migration this returns false and Project skips the
// reject guard — a mis-ordered deploy degrades to the pre-D-038 unguarded
// behavior instead of failing every charged projection.
func (s *Store) reviewSchemaReady(ctx context.Context) bool {
	s.reviewProbeOnce.Do(func() {
		var n int
		if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_name='donations' AND column_name='review_status'`).Scan(&n); err == nil && n > 0 {
			s.reviewReady.Store(true)
		}
	})
	return s.reviewReady.Load()
}

// skipRejectedCharge implements the D-038 charge/reject race guard. The
// charged projection and the takedown arrive on independent NATS consumers
// with no cross-subject ordering, and the decide transaction runs at
// read-committed, so a plain read here could not serialize against it. This
// FOR KEY SHARE read locks against the decide transaction's FOR UPDATE row
// lock: if the reject already committed, the charged frame is skipped (the
// takedown delete ran first and a later projection must not re-air the
// donation); if the projection wins the lock, the takedown's DELETE removes
// the frame after this commit.
func (s *Store) skipRejectedCharge(ctx context.Context, tx *sql.Tx, eventType string, data json.RawMessage) (bool, error) {
	if eventType != "donation.charged.v1" || !s.reviewSchemaReady(ctx) {
		return false, nil
	}
	var d struct {
		DonationID string `json:"donation_id"`
	}
	if err := json.Unmarshal(data, &d); err != nil || d.DonationID == "" {
		return false, nil
	}
	var reviewStatus string
	err := tx.QueryRowContext(ctx, `SELECT review_status FROM donations WHERE id=$1::uuid FOR KEY SHARE`, d.DonationID).Scan(&reviewStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return reviewStatus == "REJECTED", nil
}
func (s *Store) HighWater(ctx context.Context, streamer string) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq),0) FROM ws_projection_frames WHERE streamer_id=$1`, streamer).Scan(&n)
	return n, err
}
func (s *Store) Replay(ctx context.Context, streamer string, since int64) ([]Frame, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT payload FROM ws_projection_frames WHERE streamer_id=$1 AND seq>$2 AND (expires_at IS NULL OR expires_at>NOW()) ORDER BY seq`, streamer, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var a []Frame
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		var f Frame
		if err = json.Unmarshal(b, &f); err != nil {
			return nil, err
		}
		a = append(a, f)
	}
	return a, rows.Err()
}
func (s *Store) Oldest(ctx context.Context, streamer string) (int64, error) {
	var n sql.NullInt64
	err := s.DB.QueryRowContext(ctx, `SELECT MIN(seq) FROM ws_projection_frames WHERE streamer_id=$1 AND (expires_at IS NULL OR expires_at>NOW())`, streamer).Scan(&n)
	if !n.Valid {
		return 0, err
	}
	return n.Int64, err
}
