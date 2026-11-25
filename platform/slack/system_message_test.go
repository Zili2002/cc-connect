package slack

import (
	"fmt"
	"testing"
	"time"

	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"

	"github.com/chenhg5/cc-connect/core"
)

func channelMessageEvent(ev *slackevents.MessageEvent) socketmode.Event {
	return socketmode.Event{
		Type: socketmode.EventTypeEventsAPI,
		Data: slackevents.EventsAPIEvent{
			Type: slackevents.CallbackEvent,
			InnerEvent: slackevents.EventsAPIInnerEvent{
				Type: "message",
				Data: ev,
			},
		},
	}
}

func newDispatchCountingPlatform(calls *int) *Platform {
	p := &Platform{
		botUserID:        "UBOT",
		channelNameCache: map[string]string{"C1": "general"},
		handler:          func(core.Platform, *core.Message) { *calls++ },
	}
	p.userNameCache.Store("U1", "Alice")
	p.userNameCache.Store("UBOT", "CC-Connect")
	return p
}

// Regression: with message.channels subscribed, Slack delivers channel system
// events (join, purpose, topic...) as messages attributed to a user and without
// a bot_id. They must not reach the agent as prompts.
func TestHandleEvent_ChannelSystemMessagesIgnored(t *testing.T) {
	cases := []struct {
		name    string
		user    string
		subtype string
		want    int
	}{
		{"bot joined channel", "UBOT", "channel_join", 0},
		{"user invited to channel", "U1", "channel_join", 0},
		{"purpose set", "UBOT", "channel_purpose", 0},
		{"topic set", "U1", "channel_topic", 0},
		{"pinned item", "U1", "pinned_item", 0},
		{"own plain message without bot_id", "UBOT", "", 0},
		{"plain user message", "U1", "", 1},
		{"user message with file", "U1", "file_share", 1},
		{"thread reply also sent to channel", "U1", "thread_broadcast", 1},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			p := newDispatchCountingPlatform(&calls)
			p.handleEvent(channelMessageEvent(&slackevents.MessageEvent{
				Type:        "message",
				SubType:     tc.subtype,
				User:        tc.user,
				Text:        "<@" + tc.user + "> has joined the channel",
				Channel:     "C1",
				ChannelType: "channel",
				TimeStamp:   fmt.Sprintf("%d.%06d", time.Now().Unix(), 300+i),
			}))
			if calls != tc.want {
				t.Fatalf("dispatched %d messages, want %d", calls, tc.want)
			}
		})
	}
}

func TestIsSystemMessageSubtype(t *testing.T) {
	for _, s := range []string{"", "file_share", "thread_broadcast", "me_message"} {
		if isSystemMessageSubtype(s) {
			t.Fatalf("%q is user content", s)
		}
	}
	for _, s := range []string{"channel_join", "channel_leave", "channel_purpose", "channel_topic", "channel_name", "pinned_item", "bot_message"} {
		if !isSystemMessageSubtype(s) {
			t.Fatalf("%q is a system event", s)
		}
	}
}
