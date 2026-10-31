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

package expander

import (
	"sync"

	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

type synchronizedStrategy struct {
	mu       sync.Mutex
	strategy Strategy
}

// Synchronized permits read-only planning and the main loop to share the same
// policy without assuming every expander's internal caches are concurrency-safe.
func Synchronized(strategy Strategy) Strategy { return &synchronizedStrategy{strategy: strategy} }

func (s *synchronizedStrategy) BestOption(options []Option, nodes map[string]*framework.NodeInfo) *Option {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.strategy.BestOption(options, nodes)
}
