package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path"
	"time"

	"github.com/Fishwaldo/restic-nats-server/protocol"
//	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

type WorkerData struct {
	id     int
	be     *Backend
	cancel context.CancelFunc
}

func (wd *WorkerData) Run() error {
	for {
		var ctx context.Context
		ctx, wd.cancel = context.WithCancel(context.Background())
		netctx, _ := context.WithTimeout(ctx, 1*time.Second)
		select {
		case <-wd.be.t.Dying():
			fmt.Printf("Killing Worker %d\n", wd.id)
			wd.cancel()
			return nil
		default:
		}
		//wd.be.mx.Lock()
		msg, err := wd.be.buCommands.NextMsgWithContext(netctx)
		//wd.be.mx.Unlock()
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				fmt.Printf("id: %d Error: %s %t\n", wd.id, err, err)
				return err
			}
		}
		if msg != nil {
			jobctx, _ := context.WithTimeout(ctx, 120 * time.Second)
			fmt.Printf("id: %d Message: %s %s\n", wd.id, msg.Subject, msg.Sub.Queue)
			start := time.Now()

			log := func(msg string, args ...interface{}) {
				fmt.Printf("ID: %d - %s\n", wd.id, fmt.Sprintf(msg, args...))
			}

			msg, err := protocol.ChunkReadRequestMsgWithContext(jobctx, wd.be.conn, msg, log)
			if err != nil {
				fmt.Printf("ID: %d - ChunkedRead Failed: %s\n", wd.id, err)
				continue
			}

			operation := msg.Header.Get("X-RNS-OP")
			switch operation {
			case "open":
				var oo protocol.OpenOp
				or := protocol.OpenResult{Ok: false}

				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &oo); err != nil {
					fmt.Printf("ID: %d - Decode Failed: %s\n", wd.id, err)
				}

				or.Ok, err = wd.OpenRepo(oo)
				if err != nil {
					or.Ok = false
					fmt.Printf("ID: %d - OpenOp: Error: %s\n", wd.id, err)
				}

				replymsg := protocol.NewRNSMsg(msg.Reply)

				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, or)
				if err != nil {
					fmt.Printf("ID %d - Encode Failed: %s\n", wd.id, err)
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			case "stat":
				var so protocol.StatOp
				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &so); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				fi, err := wd.Stat(so)
				var sr protocol.StatResult
				if err != nil {
					fmt.Printf("Stat: Error: %s\n", err)
					sr.Ok = false
				} else {
					sr.Ok = true
					sr.Size = fi.Size()
					sr.Name = fi.Name()
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, sr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			case "mkdir":

				var mo protocol.MkdirOp
				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &mo); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var mr protocol.MkdirResult
				err := wd.Mkdir(mo)
				if err != nil {
					fmt.Printf("Mkdir: Error: %s\n", err)
					mr.Ok = false
				} else {
					mr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, mr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			case "save":
				var so protocol.SaveOp
				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &so); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var sr protocol.SaveResult
				err := wd.Save(so)
				if err != nil {
					fmt.Printf("Save: Error: %s\n", err)
					sr.Ok = false
				} else {
					sr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, sr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			case "list":
				var lo protocol.ListOp
				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &lo); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var lr protocol.ListResult
				lr, err = wd.List(lo)
				if err != nil {
					fmt.Printf("List: Error: %s\n", err)
					lr.Ok = false
				} else {
					lr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, lr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			case "load":
				var lo protocol.LoadOp
				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &lo); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var lr protocol.LoadResult
				lr, err = wd.Load(lo)
				if err != nil {
					fmt.Printf("List: Error: %s\n", err)
					lr.Ok = false
				} else {
					lr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, lr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			case "remove":
				var ro protocol.RemoveOp
				if err := wd.be.enc.Decode(msg.Subject, msg.Data, &ro); err != nil {
					return errors.Wrap(err, "Decode Failed")
				}

				var rr protocol.RemoveResult
				rr, err = wd.Remove(ro)
				if err != nil {
					fmt.Printf("List: Error: %s\n", err)
					rr.Ok = false
				} else {
					rr.Ok = true
				}
				replymsg := protocol.NewRNSMsg(msg.Reply)
				replymsg.Data, err = wd.be.enc.Encode(msg.Reply, rr)
				if err != nil {
					return errors.Wrap(err, "Encode Failed")
				}
				if err = protocol.ChunkSendReplyMsgWithContext(ctx, wd.be.conn, msg, replymsg, log); err != nil {
					fmt.Printf("ID %d - ChunkReplyRequestMsgWithContext Failed: %s\n", wd.id, err)
				}
			}
			fmt.Printf("id: %d Command %s Took %s\n\n", wd.id, operation, time.Since(start))
		}
	}
	return nil
}

func (wd *WorkerData) OpenRepo(oo protocol.OpenOp) (bool, error) {
	fs, err := FSStat(oo.Bucket)
	if err != nil {
		return false, errors.New("Failed to Open Repository")
	}
	return fs.IsDir(), nil
}

func (wd *WorkerData) Stat(so protocol.StatOp) (fs.FileInfo, error) {
	fs, err := FSStat(path.Join(so.Bucket, so.Filename))
	if err != nil {
		return nil, errors.Wrap(err, "Stat")
	}
	return fs, nil
}
func (wd *WorkerData) Mkdir(mo protocol.MkdirOp) error {
	path := path.Join(mo.Bucket, mo.Dir)
	if err := FSMkDir(path); err != nil {
		fmt.Printf("Mkdir Failed: %s", err)
		return err
	}
	return nil
}

func (wd *WorkerData) Save(so protocol.SaveOp) error {
	path := path.Join(so.Bucket, so.Dir, so.Name)
	len, err := FSSave(path, &so.Data, so.Offset)
	if err != nil {
		fmt.Printf("Save Failed: %s", err)
		return err
	}
	if len != so.PacketSize {
		fmt.Printf("Packetsize != writtensize")
		return errors.New("Packetsize != Writtensize")
	}
	return nil
}

func (wd *WorkerData) List(lo protocol.ListOp) (protocol.ListResult, error) {
	var result protocol.ListResult
	fi, err := FSListFiles(path.Join(lo.Bucket, lo.BaseDir), lo.SubDir)
	if err != nil {
		return protocol.ListResult{Ok: false}, errors.Wrap(err, "ListFiles")
	}
	result.Ok = true
	result.FI = fi
	return result, nil
}

func (wd *WorkerData) Load(lo protocol.LoadOp) (protocol.LoadResult, error) {
	var result protocol.LoadResult
	rd, err := FSLoadFile(path.Join(lo.Bucket, lo.Dir, lo.Name))
	if err != nil {
		return protocol.LoadResult{Ok: false}, errors.Wrap(err, "LoadFile")
	}
	defer rd.Close()
	if lo.Offset > 0 {
		_, err = rd.Seek(lo.Offset, 0)
		if err != nil {
			return protocol.LoadResult{Ok: false}, errors.Wrap(err, "Seek")
		}
	}
	if lo.Length > 0 {
		result.Data = make([]byte, lo.Length)
		len, err := rd.Read(result.Data)
		if err != nil {
			return protocol.LoadResult{Ok: false}, errors.Wrap(err, "Read")
		}
		if len != lo.Length {
			return protocol.LoadResult{Ok: false}, errors.Errorf("Requested Length %d != Actual Length %d", lo.Length, len)
		}
	} else {
		result.Data, err = io.ReadAll(rd)
		if err != nil {
			return protocol.LoadResult{Ok: false}, errors.Wrap(err, "ReadAll")
		}
	}
	//fmt.Printf("%+v\n", result)
	return result, nil
}

func (wd *WorkerData) Remove(ro protocol.RemoveOp) (protocol.RemoveResult, error) {
	var result protocol.RemoveResult
	if err := FSRemove(path.Join(ro.Bucket, ro.Dir, ro.Name)); err != nil {
		return protocol.RemoveResult{Ok: false}, errors.Wrap(err, "FSRemove")
	}
	result.Ok = true
	return result, nil
}
