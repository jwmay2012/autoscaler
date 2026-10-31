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

// Package externalpolicy connects optional capacity policy to native planning.
package externalpolicy

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/yaml"
)

// Config is intentionally independent of any policy controller or cloud.
// Mode applies on the CA side: observation cannot influence native decisions.
type Config struct {
	APIVersion          string `json:"apiVersion"`
	Mode                string `json:"mode"`
	Endpoint            string `json:"endpoint"`
	ServerName          string `json:"serverName"`
	CAFile              string `json:"caFile"`
	ClientCertFile      string `json:"clientCertFile"`
	ClientKeyFile       string `json:"clientKeyFile"`
	ManagedNodeSelector string `json:"managedNodeSelector"`
	// Optional controller group/kinds whose unfinished Pods must not be
	// interrupted by managed voluntary scale-down, unless explicitly evictable.
	ProtectedControllers []string      `json:"protectedControllers"`
	Timeout              string        `json:"timeout"`
	Planner              PlannerConfig `json:"planner"`
}

// PlannerConfig controls the optional mutually authenticated read-only server.
type PlannerConfig struct {
	Listen       string `json:"listen"`
	CertFile     string `json:"certFile"`
	KeyFile      string `json:"keyFile"`
	ClientCAFile string `json:"clientCAFile"`
}

func ReadConfig(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := yaml.UnmarshalStrict(data, &config); err != nil {
		return Config{}, err
	}
	if config.APIVersion != "autoscaling.policy/v1alpha1" || (config.Mode != "observe" && config.Mode != "enforce") || config.Endpoint == "" || config.CAFile == "" || config.ManagedNodeSelector == "" {
		return Config{}, fmt.Errorf("external policy requires apiVersion, observe/enforce mode, endpoint, CA file and a nonempty managed Node selector")
	}
	if _, err := labels.Parse(config.ManagedNodeSelector); err != nil {
		return Config{}, err
	}
	if config.Timeout == "" {
		config.Timeout = "1s"
	}
	if duration, err := time.ParseDuration(config.Timeout); err != nil || duration <= 0 || duration > 5*time.Second {
		return Config{}, fmt.Errorf("external policy timeout must be between zero and five seconds")
	}
	if config.Planner.Listen != "" && (config.Planner.CertFile == "" || config.Planner.KeyFile == "" || config.Planner.ClientCAFile == "") {
		return Config{}, fmt.Errorf("capacity planner requires a server certificate, key and client CA")
	}
	if (config.ClientCertFile == "") != (config.ClientKeyFile == "") {
		return Config{}, fmt.Errorf("external policy client certificate and key must be supplied together")
	}
	return config, nil
}

func certificatePool(filename string) (*x509.CertPool, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("no CA certificates in %s", filename)
	}
	return pool, nil
}

func clientTLS(config Config) (*tls.Config, error) {
	pool, err := certificatePool(config.CAFile)
	if err != nil {
		return nil, err
	}
	result := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: config.ServerName}
	if config.ClientCertFile != "" {
		if _, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile); err != nil {
			return nil, err
		}
		result.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			certificate, err := tls.LoadX509KeyPair(config.ClientCertFile, config.ClientKeyFile)
			return &certificate, err
		}
	}
	return result, nil
}

func serverTLS(config PlannerConfig) (*tls.Config, error) {
	pool, err := certificatePool(config.ClientCAFile)
	if err != nil {
		return nil, err
	}
	if _, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile); err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			return &certificate, err
		}}, nil
}
