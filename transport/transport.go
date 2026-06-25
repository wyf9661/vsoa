package transport

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"time"
)

type TLSOptions struct {
	Hostname          string
	LoadDefaultCerts  bool
	CACert            string
	Cert              string
	Key               string
	Password          string
	InsecureSkipVerify bool
	HandshakeErrorLog bool
	RequireClientCert bool
}

func NewClientTLSConfig(opt TLSOptions) (*tls.Config, string, error) {
	cfg := &tls.Config{InsecureSkipVerify: opt.InsecureSkipVerify, ServerName: opt.Hostname}
	if opt.CACert != "" {
		pem, err := os.ReadFile(opt.CACert)
		if err != nil {
			return nil, "", err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		cfg.RootCAs = pool
	}
	if opt.Cert != "" {
		cert, err := tls.LoadX509KeyPair(opt.Cert, opt.Key)
		if err != nil {
			return nil, "", err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, opt.Hostname, nil
}

func NewServerTLSConfig(opt TLSOptions) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(opt.Cert, opt.Key)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if opt.CACert != "" {
		pem, err := os.ReadFile(opt.CACert)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(pem)
		cfg.ClientCAs = pool
		if opt.RequireClientCert {
			cfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	return cfg, nil
}

func Handshake(conn *tls.Conn, timeout time.Duration) error {
	_ = conn.SetDeadline(time.Now().Add(timeout))
	defer conn.SetDeadline(time.Time{})
	if err := conn.Handshake(); err != nil {
		return err
	}
	return nil
}

func WriteFull(conn net.Conn, data []byte, timeout time.Duration) error {
	if timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
		defer conn.SetWriteDeadline(time.Time{})
	}
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 {
			return errors.New("short write")
		}
		data = data[n:]
	}
	return nil
}
