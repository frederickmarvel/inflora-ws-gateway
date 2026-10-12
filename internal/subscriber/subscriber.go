package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/auth"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/seq"
	gatewayws "github.com/frederickmarvel/inflora-ws-gateway/internal/ws"
)

type Donation struct {
	N     *events.NATSSubscriber
	Store *seq.Store
	Hub   *gatewayws.Hub
}

func (d Donation) Run(ctx context.Context) error {
	return d.N.Subscribe(ctx, events.DonationCharged+".>", "ws-gateway-donation-charged", func(ctx context.Context, e events.Envelope) error {
		if e.StreamerID == nil {
			return fmt.Errorf("missing streamer_id")
		}
		var p events.DonationChargedEvent
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		b, _ := json.Marshal(map[string]any{
			"donation_id": p.DonationID, "amount_idr": p.AmountIDR, "mdr_idr": p.MDRIDR,
			"gross_charged_idr": p.GrossChargedIDR, "platform_fee_idr": p.PlatformFeeIDR,
			"net_idr": p.NetIDR, "settlement_status": p.SettlementStatus,
			"donor_display_name": p.DonorDisplayName, "is_anonymous": p.IsAnonymous,
			"message": p.Message, "voice_url": p.VoiceURL, "voice_duration_sec": p.VoiceDurationSec,
			"youtube_url": p.YouTubeURL, "youtube_start_sec": p.YouTubeStartSec,
			"youtube_end_sec": p.YouTubeEndSec, "display_duration_sec": p.DisplayDurationSec,
			"payment_method": p.PaymentMethod,
		})
		f, dup, err := d.Store.Project(ctx, "ws-gateway.donation-charged", e.EventID, e.EventType, *e.StreamerID, b)
		if err == nil && !dup {
			d.Hub.Broadcast(*e.StreamerID, f)
		}
		return err
	})
}

type Rotation struct {
	N    *events.NATSSubscriber
	Auth *auth.Validator
}

func (r Rotation) Run(ctx context.Context) error {
	return r.N.Subscribe(ctx, events.OverlayTokenRotated+".>", "ws-gateway-overlay-token-rotated", func(_ context.Context, e events.Envelope) error {
		var p events.OverlayTokenRotatedEvent
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return err
		}
		r.Auth.InvalidateStreamer(p.StreamerID)
		return nil
	})
}
