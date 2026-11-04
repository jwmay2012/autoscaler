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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

// planningGroup cannot mutate the underlying provider, even if a future
// estimator starts calling additional methods on its NodeGroup argument.
type planningGroup struct {
	id                       string
	minimum, maximum, target int
	options                  config.NodeGroupAutoscalingOptions
	template                 *framework.NodeInfo
	safe                     bool
	delta                    map[string]int64
}

func (g *planningGroup) Id() string               { return g.id }
func (g *planningGroup) Debug() string            { return g.id }
func (g *planningGroup) MinSize() int             { return g.minimum }
func (g *planningGroup) MaxSize() int             { return g.maximum }
func (g *planningGroup) TargetSize() (int, error) { return g.target, nil }
func (g *planningGroup) Exist() bool              { return true }
func (g *planningGroup) Autoprovisioned() bool    { return false }
func (g *planningGroup) Nodes() ([]cloudprovider.Instance, error) {
	return nil, fmt.Errorf("provider instances are unavailable in read-only planning")
}
func (g *planningGroup) TemplateNodeInfo() (*framework.NodeInfo, error) {
	return g.template.DeepCopy(), nil
}
func (g *planningGroup) GetOptions(config.NodeGroupAutoscalingOptions) (*config.NodeGroupAutoscalingOptions, error) {
	copy := g.options
	return &copy, nil
}
func (*planningGroup) IncreaseSize(int) error                { return fmt.Errorf("read-only planner") }
func (*planningGroup) AtomicIncreaseSize(int) error          { return fmt.Errorf("read-only planner") }
func (*planningGroup) DecreaseTargetSize(int) error          { return fmt.Errorf("read-only planner") }
func (*planningGroup) DeleteNodes([]*corev1.Node) error      { return fmt.Errorf("read-only planner") }
func (*planningGroup) ForceDeleteNodes([]*corev1.Node) error { return fmt.Errorf("read-only planner") }
func (*planningGroup) Delete() error                         { return fmt.Errorf("read-only planner") }
func (*planningGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, fmt.Errorf("read-only planner")
}
