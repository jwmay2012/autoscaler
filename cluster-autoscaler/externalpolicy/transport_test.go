/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package externalpolicy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
)

const (
	plannerServerName = "planner.example.test"
	policyServerName  = "policy.example.test"
	clientCommonName  = "client.example.test"
)

var testSerial atomic.Int64

type testAuthority struct {
	certificate *x509.Certificate
	key         *ecdsa.PrivateKey
	file        string
}

type issuedPair struct {
	certFile, keyFile string
	serial            *big.Int
}

func newTestAuthority(t *testing.T, name string) *testAuthority {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(testSerial.Add(1)), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "ca.pem")
	writePEM(t, file, "CERTIFICATE", der)
	return &testAuthority{certificate: certificate, key: key, file: file}
}

// issue writes a leaf pair. Reusing dir/name overwrites the files in place,
// which is how certificate rotation is exercised.
func (a *testAuthority) issue(t *testing.T, dir, name, commonName string, usage x509.ExtKeyUsage, notAfter time.Time, dnsNames ...string) issuedPair {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial := big.NewInt(testSerial.Add(1))
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: commonName}, DNSNames: dnsNames,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage}}
	der, err := x509.CreateCertificate(rand.Reader, template, a.certificate, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pair := issuedPair{certFile: filepath.Join(dir, name+".pem"), keyFile: filepath.Join(dir, name+"-key.pem"), serial: serial}
	writePEM(t, pair.certFile, "CERTIFICATE", der)
	writePEM(t, pair.keyFile, "EC PRIVATE KEY", keyDER)
	return pair
}

func writePEM(t *testing.T, file, kind string, der []byte) {
	t.Helper()
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func loadPair(t *testing.T, pair issuedPair) tls.Certificate {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(pair.certFile, pair.keyFile)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func startTestPlanner(t *testing.T, authority, clients *testAuthority) string {
	t.Helper()
	pair := authority.issue(t, t.TempDir(), "planner", plannerServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), plannerServerName)
	address := freeAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// A request rejected by validation proves that transport authentication
	// completed and the handler ran, without needing a cluster snapshot.
	planner := &Planner{gate: make(chan struct{}, 1)}
	if err := planner.Start(ctx, PlannerConfig{Listen: address, CertFile: pair.certFile, KeyFile: pair.keyFile, ClientCAFile: clients.file}); err != nil {
		t.Fatal(err)
	}
	return address
}

func callPlanner(t *testing.T, address string, config *tls.Config) error {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(config)))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = api.NewCapacityPlannerClient(connection).PlanDrain(ctx, &api.DrainRequest{})
	return err
}

func pool(t *testing.T, authorities ...*testAuthority) *x509.CertPool {
	t.Helper()
	result := x509.NewCertPool()
	for _, authority := range authorities {
		result.AddCert(authority.certificate)
	}
	return result
}

func handlerReached(err error) bool { return status.Code(err) == codes.InvalidArgument }

func transportRejected(err error) bool {
	code := status.Code(err)
	return err != nil && (code == codes.Unavailable || code == codes.DeadlineExceeded)
}

func TestPlannerServerRequiresTrustedClientCertificate(t *testing.T) {
	serverAuthority, clientAuthority, untrusted := newTestAuthority(t, "server-ca"), newTestAuthority(t, "client-ca"), newTestAuthority(t, "untrusted-ca")
	address := startTestPlanner(t, serverAuthority, clientAuthority)
	dir := t.TempDir()
	valid := clientAuthority.issue(t, dir, "valid", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	foreign := untrusted.issue(t, dir, "foreign", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	expired := clientAuthority.issue(t, dir, "expired", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(-time.Minute))
	serverOnly := clientAuthority.issue(t, dir, "server-only", clientCommonName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour))

	for _, test := range []struct {
		name   string
		config *tls.Config
		accept bool
	}{
		{"trusted client certificate", &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: plannerServerName, Certificates: []tls.Certificate{loadPair(t, valid)}}, true},
		{"missing client certificate", &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: plannerServerName}, false},
		{"client certificate from an untrusted authority", &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: plannerServerName, Certificates: []tls.Certificate{loadPair(t, foreign)}}, false},
		{"expired client certificate", &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: plannerServerName, Certificates: []tls.Certificate{loadPair(t, expired)}}, false},
		{"certificate without client authentication usage", &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: plannerServerName, Certificates: []tls.Certificate{loadPair(t, serverOnly)}}, false},
		{"client does not trust the planner", &tls.Config{RootCAs: pool(t, untrusted), ServerName: plannerServerName, Certificates: []tls.Certificate{loadPair(t, valid)}}, false},
		{"client expects another server name", &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: "other.example.test", Certificates: []tls.Certificate{loadPair(t, valid)}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := callPlanner(t, address, test.config)
			if test.accept && !handlerReached(err) {
				t.Fatalf("authenticated client did not reach the planner: %v", err)
			}
			if !test.accept && !transportRejected(err) {
				t.Fatalf("unauthenticated client was not rejected by transport: %v", err)
			}
		})
	}
}

func TestPlannerServerRejectsUnusableConfiguration(t *testing.T) {
	serverAuthority, clientAuthority := newTestAuthority(t, "server-ca"), newTestAuthority(t, "client-ca")
	dir := t.TempDir()
	pair := serverAuthority.issue(t, dir, "planner", plannerServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), plannerServerName)
	other := serverAuthority.issue(t, dir, "other", plannerServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), plannerServerName)
	empty := filepath.Join(dir, "empty.pem")
	if err := os.WriteFile(empty, []byte("no certificates here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, config := range map[string]PlannerConfig{
		"client CA without certificates": {Listen: freeAddress(t), CertFile: pair.certFile, KeyFile: pair.keyFile, ClientCAFile: empty},
		"missing client CA":              {Listen: freeAddress(t), CertFile: pair.certFile, KeyFile: pair.keyFile, ClientCAFile: filepath.Join(dir, "absent.pem")},
		"mismatched key":                 {Listen: freeAddress(t), CertFile: pair.certFile, KeyFile: other.keyFile, ClientCAFile: clientAuthority.file},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if err := (&Planner{gate: make(chan struct{}, 1)}).Start(ctx, config); err == nil {
				t.Fatal("planner served with an unusable transport configuration")
			}
		})
	}
}

type testPolicyServer struct {
	api.UnimplementedScaleDownPolicyServer
	clients chan string
}

func (s *testPolicyServer) OrderCandidates(ctx context.Context, request *api.OrderCandidatesRequest) (*api.OrderCandidatesResponse, error) {
	name := ""
	if source, ok := peer.FromContext(ctx); ok {
		if info, ok := source.AuthInfo.(credentials.TLSInfo); ok && len(info.State.PeerCertificates) > 0 {
			name = info.State.PeerCertificates[0].Subject.CommonName
		}
	}
	select {
	case s.clients <- name:
	default:
	}
	return &api.OrderCandidatesResponse{SnapshotId: request.SnapshotId, PolicyRevision: "revision"}, nil
}

func startTestPolicyServer(t *testing.T, authority, clients *testAuthority) (string, *testPolicyServer) {
	t.Helper()
	pair := authority.issue(t, t.TempDir(), "policy", policyServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), policyServerName)
	config := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{loadPair(t, pair)}}
	if clients != nil {
		config.ClientAuth, config.ClientCAs = tls.RequireAndVerifyClientCert, pool(t, clients)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)))
	service := &testPolicyServer{clients: make(chan string, 8)}
	api.RegisterScaleDownPolicyServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String(), service
}

func policyConfig(address, caFile, serverName string) Config {
	return Config{APIVersion: "autoscaling.policy/v1alpha1", Mode: "enforce", Endpoint: address, ServerName: serverName, CAFile: caFile,
		ManagedNodeSelector: "example.com/managed=true", Timeout: "1s"}
}

func orderOnce(t *testing.T, config Config) error {
	t.Helper()
	client, err := NewClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CleanUp()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := client.rpc.OrderCandidates(ctx, &api.OrderCandidatesRequest{SnapshotId: "snapshot"})
	if err == nil && (response.SnapshotId != "snapshot" || response.PolicyRevision == "") {
		t.Fatalf("unexpected policy response: %+v", response)
	}
	return err
}

func TestPolicyClientVerifiesServerAuthorityAndName(t *testing.T) {
	authority, untrusted := newTestAuthority(t, "policy-ca"), newTestAuthority(t, "untrusted-ca")
	address, _ := startTestPolicyServer(t, authority, nil)
	for _, test := range []struct {
		name   string
		config Config
		accept bool
	}{
		{"trusted authority and expected name", policyConfig(address, authority.file, policyServerName), true},
		{"untrusted authority", policyConfig(address, untrusted.file, policyServerName), false},
		{"unexpected server name", policyConfig(address, authority.file, "other.example.test"), false},
		// Without an explicit name the address host is verified; the policy
		// certificate has no IP subject alternative name.
		{"implicit address name", policyConfig(address, authority.file, ""), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := orderOnce(t, test.config)
			if test.accept && err != nil {
				t.Fatalf("trusted policy server was rejected: %v", err)
			}
			if !test.accept && !transportRejected(err) {
				t.Fatalf("untrusted policy server was accepted: %v", err)
			}
		})
	}
}

func TestPolicyClientPresentsConfiguredClientCertificate(t *testing.T) {
	serverAuthority, clientAuthority, untrusted := newTestAuthority(t, "policy-ca"), newTestAuthority(t, "client-ca"), newTestAuthority(t, "untrusted-ca")
	address, service := startTestPolicyServer(t, serverAuthority, clientAuthority)
	dir := t.TempDir()
	valid := clientAuthority.issue(t, dir, "valid", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	foreign := untrusted.issue(t, dir, "foreign", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))

	anonymous := policyConfig(address, serverAuthority.file, policyServerName)
	if err := orderOnce(t, anonymous); !transportRejected(err) {
		t.Fatalf("mutually authenticated policy server accepted a client without a certificate: %v", err)
	}
	rejected := anonymous
	rejected.ClientCertFile, rejected.ClientKeyFile = foreign.certFile, foreign.keyFile
	if err := orderOnce(t, rejected); !transportRejected(err) {
		t.Fatalf("policy server accepted an untrusted client certificate: %v", err)
	}
	accepted := anonymous
	accepted.ClientCertFile, accepted.ClientKeyFile = valid.certFile, valid.keyFile
	if err := orderOnce(t, accepted); err != nil {
		t.Fatalf("configured client certificate was not presented: %v", err)
	}
	select {
	case name := <-service.clients:
		if name != clientCommonName {
			t.Fatalf("policy server saw client identity %q", name)
		}
	case <-time.After(time.Second):
		t.Fatal("policy server did not record the authenticated client")
	}
}

func TestPolicyClientRejectsUnusableConfiguration(t *testing.T) {
	authority := newTestAuthority(t, "policy-ca")
	dir := t.TempDir()
	first := authority.issue(t, dir, "first", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	second := authority.issue(t, dir, "second", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	empty := filepath.Join(dir, "empty.pem")
	if err := os.WriteFile(empty, []byte("no certificates here\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := policyConfig("127.0.0.1:1", authority.file, policyServerName)
	noCA := base
	noCA.CAFile = empty
	mismatched := base
	mismatched.ClientCertFile, mismatched.ClientKeyFile = first.certFile, second.keyFile
	for name, config := range map[string]Config{"CA file without certificates": noCA, "mismatched client key": mismatched} {
		t.Run(name, func(t *testing.T) {
			if client, err := NewClient(config); err == nil {
				client.CleanUp()
				t.Fatal("client was created with an unusable transport configuration")
			}
		})
	}

	file := filepath.Join(dir, "config.yaml")
	write := func(body string) {
		if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("apiVersion: autoscaling.policy/v1alpha1\nmode: enforce\nendpoint: policy.example.test:443\ncaFile: " + authority.file +
		"\nmanagedNodeSelector: example.com/managed=true\nclientCertFile: " + first.certFile + "\n")
	if _, err := ReadConfig(file); err == nil {
		t.Fatal("configuration accepted a client certificate without its key")
	}
	write("apiVersion: autoscaling.policy/v1alpha1\nmode: enforce\nendpoint: policy.example.test:443\ncaFile: " + authority.file +
		"\nmanagedNodeSelector: example.com/managed=true\nplanner:\n  listen: 127.0.0.1:0\n  certFile: " + first.certFile + "\n  keyFile: " + first.keyFile + "\n")
	if _, err := ReadConfig(file); err == nil {
		t.Fatal("configuration accepted a planner without a client CA")
	}
}

// Leaf certificates are reread for every handshake, so routine rotation of
// either side's leaf does not require a restart.
func TestPlannerServerCertificateIsReloadedForNewConnections(t *testing.T) {
	serverAuthority, clientAuthority := newTestAuthority(t, "server-ca"), newTestAuthority(t, "client-ca")
	dir := t.TempDir()
	first := serverAuthority.issue(t, dir, "planner", plannerServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), plannerServerName)
	client := clientAuthority.issue(t, dir, "client", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	config, err := serverTLS(PlannerConfig{CertFile: first.certFile, KeyFile: first.keyFile, ClientCAFile: clientAuthority.file})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.(*tls.Conn).Handshake()
			_ = connection.Close()
		}
	}()
	served := func() *big.Int {
		connection, err := tls.Dial("tcp", listener.Addr().String(), &tls.Config{RootCAs: pool(t, serverAuthority), ServerName: plannerServerName, Certificates: []tls.Certificate{loadPair(t, client)}})
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		return connection.ConnectionState().PeerCertificates[0].SerialNumber
	}
	if serial := served(); serial.Cmp(first.serial) != 0 {
		t.Fatalf("planner served serial %v, expected %v", serial, first.serial)
	}
	rotated := serverAuthority.issue(t, dir, "planner", plannerServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), plannerServerName)
	if serial := served(); serial.Cmp(rotated.serial) != 0 {
		t.Fatalf("rotated planner certificate was not served: got %v, expected %v", serial, rotated.serial)
	}
}

func TestPolicyClientCertificateIsReloadedForNewConnections(t *testing.T) {
	serverAuthority, clientAuthority := newTestAuthority(t, "policy-ca"), newTestAuthority(t, "client-ca")
	dir := t.TempDir()
	server := serverAuthority.issue(t, dir, "policy", policyServerName, x509.ExtKeyUsageServerAuth, time.Now().Add(time.Hour), policyServerName)
	first := clientAuthority.issue(t, dir, "client", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{loadPair(t, server)}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool(t, clientAuthority)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	seen := make(chan *big.Int, 4)
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			conn := connection.(*tls.Conn)
			if conn.Handshake() == nil && len(conn.ConnectionState().PeerCertificates) > 0 {
				seen <- conn.ConnectionState().PeerCertificates[0].SerialNumber
			}
			_ = conn.Close()
		}
	}()
	config := policyConfig(listener.Addr().String(), serverAuthority.file, policyServerName)
	config.ClientCertFile, config.ClientKeyFile = first.certFile, first.keyFile
	tlsConfig, err := clientTLS(config)
	if err != nil {
		t.Fatal(err)
	}
	presented := func() *big.Int {
		connection, err := tls.Dial("tcp", listener.Addr().String(), tlsConfig)
		if err != nil {
			t.Fatal(err)
		}
		// TLS 1.3 completes the client handshake before the server verifies
		// the client certificate; wait for the server's verified observation.
		defer connection.Close()
		select {
		case serial := <-seen:
			return serial
		case <-time.After(3 * time.Second):
			t.Fatal("server did not verify a client certificate")
			return nil
		}
	}
	if serial := presented(); serial.Cmp(first.serial) != 0 {
		t.Fatalf("client presented serial %v, expected %v", serial, first.serial)
	}
	rotated := clientAuthority.issue(t, dir, "client", clientCommonName, x509.ExtKeyUsageClientAuth, time.Now().Add(time.Hour))
	if serial := presented(); serial.Cmp(rotated.serial) != 0 {
		t.Fatalf("rotated client certificate was not presented: got %v, expected %v", serial, rotated.serial)
	}
}
