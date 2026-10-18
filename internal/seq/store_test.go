package seq

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	existsQ = `SELECT EXISTS(SELECT 1 FROM ws_projection_inbox WHERE consumer_name=$1 AND event_id=$2)`
	insertQ = `INSERT INTO ws_projection_inbox(consumer_name,event_id,event_type) VALUES($1,$2,$3)`
	doneQ   = `UPDATE ws_projection_inbox SET processed_at=NOW() WHERE consumer_name=$1 AND event_id=$2`
	deleteQ = `DELETE FROM ws_projection_frames WHERE streamer_id=$1 AND payload->'data'->>'donation_id'=$2`
	probeQ  = `SELECT COUNT(*) FROM information_schema.columns WHERE table_name='donations' AND column_name='review_status'`
	lockQ   = `SELECT review_status FROM donations WHERE id=$1::uuid FOR KEY SHARE`
	seqQ    = `INSERT INTO ws_projection_sequences(streamer_id,next_seq) VALUES($1,2) ON CONFLICT(streamer_id) DO UPDATE SET next_seq=ws_projection_sequences.next_seq+1,updated_at=NOW() RETURNING next_seq-1`
	frameQ  = `INSERT INTO ws_projection_frames(streamer_id,seq,source_event_id,event_type,payload,expires_at) VALUES($1,$2,$3,$4,$5,$6)`
)

func TestRemoveDeletesFrameAndReturnsBroadcastFrame(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(existsQ)).WithArgs("ws-gateway.donation-review", "evt-1").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(regexp.QuoteMeta(insertQ)).WithArgs("ws-gateway.donation-review", "evt-1", "donation.reviewed.v1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(deleteQ)).WithArgs("streamer-1", "11111111-1111-1111-1111-111111111111").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(doneQ)).WithArgs("ws-gateway.donation-review", "evt-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s := &Store{DB: db}
	frame, changed, err := s.Remove(t.Context(), "ws-gateway.donation-review", "evt-1", "donation.reviewed.v1", "streamer-1", "11111111-1111-1111-1111-111111111111")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("changed = false, want true")
	}
	if frame.Type != "donation_removed" || frame.StreamerID != "streamer-1" {
		t.Fatalf("frame = %+v", frame)
	}
	var data map[string]string
	if err := json.Unmarshal(frame.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data["donation_id"] != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("data = %#v", data)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoveNoRowsIsQuiet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(existsQ)).WithArgs("c", "e").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(regexp.QuoteMeta(insertQ)).WithArgs("c", "e", "donation.reviewed.v1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(deleteQ)).WithArgs("s", "d").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta(doneQ)).WithArgs("c", "e").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s := &Store{DB: db}
	frame, changed, err := s.Remove(t.Context(), "c", "e", "donation.reviewed.v1", "s", "d")
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("changed = true, want false when no frame existed")
	}
	if frame.Type != "" {
		t.Fatalf("frame = %+v, want zero", frame)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestRemoveReDeliversRemovalFrameOnDuplicate closes the D-038 crash gap:
// the inbox row is committed before the broadcast, so a redelivered
// already-consumed event must re-emit the ephemeral removal frame (the
// overlay removal is idempotent).
func TestRemoveReDeliversRemovalFrameOnDuplicate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(existsQ)).WithArgs("c", "e").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectCommit()

	s := &Store{DB: db}
	frame, changed, err := s.Remove(t.Context(), "c", "e", "donation.reviewed.v1", "s", "d")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("changed = false, want true so the broadcast is re-sent")
	}
	if frame.Type != "donation_removed" || frame.StreamerID != "s" {
		t.Fatalf("frame = %+v", frame)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
// TestProjectSkipsRejectedCharge pins the D-038 charge/reject race guard:
// when the reject already committed, the charged projection skips the frame
// entirely (no seq allocation, no insert) so a late charge can never re-air
// a takedown.
func TestProjectSkipsRejectedCharge(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(existsQ)).WithArgs("ws-gateway.donation-charged", "evt-1").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(regexp.QuoteMeta(insertQ)).WithArgs("ws-gateway.donation-charged", "evt-1", "donation.charged.v1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(probeQ)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(lockQ)).WithArgs("11111111-1111-1111-1111-111111111111").WillReturnRows(sqlmock.NewRows([]string{"review_status"}).AddRow("REJECTED"))
	mock.ExpectCommit()

	s := &Store{DB: db}
	frame, changed, err := s.Project(t.Context(), "ws-gateway.donation-charged", "evt-1", "donation.charged.v1", "streamer-1", json.RawMessage(`{"donation_id":"11111111-1111-1111-1111-111111111111","amount_idr":50000}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("changed = true, want false for skipped rejected charge")
	}
	if frame.Type != "" {
		t.Fatalf("frame = %+v, want zero", frame)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestProjectProjectsCleanCharge covers the guard's pass-through: an APPROVED
// donation projects exactly as before the guard existed.
func TestProjectProjectsCleanCharge(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(existsQ)).WithArgs("ws-gateway.donation-charged", "evt-2").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(regexp.QuoteMeta(insertQ)).WithArgs("ws-gateway.donation-charged", "evt-2", "donation.charged.v1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(probeQ)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(regexp.QuoteMeta(lockQ)).WithArgs("11111111-1111-1111-1111-111111111111").WillReturnRows(sqlmock.NewRows([]string{"review_status"}).AddRow("APPROVED"))
	mock.ExpectQuery(regexp.QuoteMeta(seqQ)).WithArgs("streamer-1").WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(5))
	mock.ExpectExec(regexp.QuoteMeta(frameQ)).WithArgs("streamer-1", int64(5), "evt-2", "donation.charged.v1", sqlmock.AnyArg(), nil).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(doneQ)).WithArgs("ws-gateway.donation-charged", "evt-2").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s := &Store{DB: db}
	frame, changed, err := s.Project(t.Context(), "ws-gateway.donation-charged", "evt-2", "donation.charged.v1", "streamer-1", json.RawMessage(`{"donation_id":"11111111-1111-1111-1111-111111111111","amount_idr":50000}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("changed = true, want false")
	}
	if frame.Type != "donation" || frame.Seq != 5 {
		t.Fatalf("frame = %+v", frame)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestProjectGuardDegradesWithoutSchema pins the mis-ordered deploy case: on
// a pre-00011 database the probe returns 0 and the guard never runs (no
// locking read), so charging keeps working exactly as before D-038.
func TestProjectGuardDegradesWithoutSchema(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(existsQ)).WithArgs("ws-gateway.donation-charged", "evt-3").WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	mock.ExpectExec(regexp.QuoteMeta(insertQ)).WithArgs("ws-gateway.donation-charged", "evt-3", "donation.charged.v1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta(probeQ)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(seqQ)).WithArgs("streamer-1").WillReturnRows(sqlmock.NewRows([]string{"next_seq"}).AddRow(2))
	mock.ExpectExec(regexp.QuoteMeta(frameQ)).WithArgs("streamer-1", int64(2), "evt-3", "donation.charged.v1", sqlmock.AnyArg(), nil).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(doneQ)).WithArgs("ws-gateway.donation-charged", "evt-3").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s := &Store{DB: db}
	_, _, err = s.Project(t.Context(), "ws-gateway.donation-charged", "evt-3", "donation.charged.v1", "streamer-1", json.RawMessage(`{"donation_id":"11111111-1111-1111-1111-111111111111"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
