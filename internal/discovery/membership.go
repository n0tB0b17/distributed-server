package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Config struct {
	NodeName      string
	BindAddr      string
	Tags          map[string]string
	StartJoinAddr []string
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(hdlr Handler, cfg Config) (*Membership, error) {
	membership := &Membership{
		Config:  cfg,
		handler: hdlr,
		logger:  zap.L().Named("membership"),
	}

	if err := membership.setupSerf(); err != nil {
		return nil, err
	}

	return membership, nil
}

func (M *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", M.BindAddr)
	if err != nil {
		return err
	}

	cfg := serf.DefaultConfig()
	cfg.MemberlistConfig.BindAddr = addr.IP.String()
	cfg.MemberlistConfig.BindPort = addr.Port

	M.events = make(chan serf.Event)
	cfg.EventCh = M.events
	cfg.Tags = M.Tags
	cfg.NodeName = M.Config.NodeName

	M.serf, err = serf.Create(cfg)
	if err != nil {
		return err
	}

	go M.eventHandler()
	if M.StartJoinAddr != nil {
		_, err = M.serf.Join(M.StartJoinAddr, true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (M *Membership) eventHandler() {
	for e := range M.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, mem := range e.(serf.MemberEvent).Members {
				if M.isLocalMember(mem) {
					continue
				}

				M.handleJoin(mem)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, mem := range e.(serf.MemberEvent).Members {
				if M.isLocalMember(mem) {
					return
				}

				M.handleLeave(mem)
			}
		}
	}
}

func (M *Membership) handleJoin(mem serf.Member) {
	if err := M.handler.Join(mem.Name, mem.Tags["rpc_addr"]); err != nil {
		M.logErr(
			err,
			"Failed to join",
			mem,
		)
	}
}

func (M *Membership) handleLeave(mem serf.Member) {
	if err := M.handler.Leave(mem.Name); err != nil {
		M.logErr(
			err,
			"Failed to leave",
			mem,
		)
	}
}

func (M *Membership) isLocalMember(mem serf.Member) bool {
	return M.serf.LocalMember().Name == mem.Name
}

func (M *Membership) Mems() []serf.Member {
	return M.serf.Members()
}

func (M *Membership) Leave() error {
	return M.serf.Leave()
}

func (M *Membership) logErr(err error, msg string, mem serf.Member) {
	M.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", mem.Name),
		zap.String("rpc_addr", mem.Tags["rpc_addr"]),
	)
}
