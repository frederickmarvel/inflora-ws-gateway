package seq

import (
	"context"
	"database/sql"
	"encoding/json"
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
