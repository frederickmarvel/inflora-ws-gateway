package subscriber

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/seq"
	gatewayws "github.com/frederickmarvel/inflora-ws-gateway/internal/ws"
)

// Review consumes donation.reviewed.v1 (CONTRACT_DRIFT D-038). A REJECTED
// decision is a replay-safe takedown of an already-aired donation: the
// projected frame is deleted (so replays never re-air it) and a
// donation_removed frame is broadcast so connected overlays drop it from
// their display immediately. APPROVED decisions never emit events; the
// branch exists only as a defensive no-op.
type Review struct {
	N     *events.NATSSubscriber
	Store *seq.Store
	Hub   *gatewayws.Hub
}

func (rv Review) Run(ctx context.Context) error {
	return rv.N.Subscribe(ctx, events.DonationReviewed+".>", "ws-gateway-donation-review", func(ctx context.Context, e events.Envelope) error {
		if e.StreamerID == nil {
			return fmt.Errorf("missing streamer_id")
		}
		var p events.DonationReviewedEvent
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		if p.ReviewStatus != "REJECTED" {
			return nil
		}
		frame, changed, err := rv.Store.Remove(ctx, "ws-gateway.donation-review", e.EventID, e.EventType, *e.StreamerID, p.DonationID)
		if err == nil && changed {
			rv.Hub.Broadcast(*e.StreamerID, frame)
		}
		return err
	})
}