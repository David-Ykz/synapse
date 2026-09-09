package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

//go:embed templates/namespace.yaml
var namespaceTemplateSource string

//go:embed templates/agent.yaml
var agentTemplateSource string

//go:embed templates/broker.yaml
var brokerTemplateSource string

/*
SwarmConfig is the top-level shape of the user-authored swarm config: the
user defines their own agent images, this only describes how they connect
(via namespaces) and scale
*/
type SwarmConfig struct {
	Namespace string                 `yaml:"namespace"`
	Broker    BrokerConfig           `yaml:"broker"`
	Agents    map[string]AgentConfig `yaml:"agents"`
}

type BrokerConfig struct {
	Replicas int `yaml:"replicas"`
}

type AgentConfig struct {
	Image          string            `yaml:"image"`
	Consumes       []string          `yaml:"consumes"`
	Produces       []string          `yaml:"produces"`
	Replicas       int               `yaml:"replicas"`
	MinReplicas    int               `yaml:"min_replicas"`
	MaxReplicas    int               `yaml:"max_replicas"`
	LagPerReplica  int               `yaml:"lag_per_replica"`
	Env            map[string]string `yaml:"env"`
	EnvFromSecrets []string          `yaml:"env_from_secrets"`
}

// hpaScaleDownStabilizationSeconds limits how quickly a HorizontalPodAutoscaler will scale an
// agent back down once lag drops
const hpaScaleDownStabilizationSeconds = 60

// defaultLagPerReplica is used when an agent sets scaling bounds but no lag_per_replica
const defaultLagPerReplica = 10

// HPATemplateData holds the scaling fields for an agent's generated HorizontalPodAutoscaler
// the metric queries the broker's synapse_broker_lag External metric
type HPATemplateData struct {
	MinReplicas                   int
	MaxReplicas                   int
	LagPerReplica                 int
	ConsumeNamespaces             []string
	ScaleDownStabilizationSeconds int
}

type EnvVar struct {
	Name  string
	Value string
}

type AgentTemplateData struct {
	Namespace         string
	AgentName         string
	Image             string
	Replicas          int
	ConsumeNamespaces string // comma-joined
	ProduceNamespaces string // comma-joined
	Env               []EnvVar
	EnvFromSecrets    []string
	HPA               *HPATemplateData
}

type BrokerTemplateData struct {
	Namespace string
	Replicas  int
}

type NamespaceTemplateData struct {
	Namespace string
}

// k8sName sanitizes an agent's config key into a DNS-1123-compliant resource name
func k8sName(name string) string {
	return strings.ReplaceAll(name, "_", "-")
}

func writeTemplateToFile(tmpl *template.Template, data interface{}, path string) error {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("render template for %s: %w", path, err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("write file %s: %w", path, err)
	}
	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: parser <swarm.yaml> <generated_manifests_dir>")
		os.Exit(1)
	}
	swarmConfigPath := os.Args[1]
	generatedManifestsDir := os.Args[2]

	swarmConfigFile, err := os.ReadFile(swarmConfigPath)
	if err != nil {
		fmt.Println("Error reading swarm config file:", err)
		os.Exit(1)
	}

	var config SwarmConfig
	if err := yaml.Unmarshal(swarmConfigFile, &config); err != nil {
		fmt.Println("Error parsing swarm config:", err)
		os.Exit(1)
	}
	if config.Namespace == "" {
		config.Namespace = "default"
	}
	if config.Broker.Replicas <= 0 {
		config.Broker.Replicas = 3
	}

	namespaceTemplate := template.Must(template.New("namespace").Parse(namespaceTemplateSource))
	agentTemplate := template.Must(template.New("agent").Parse(agentTemplateSource))
	brokerTemplate := template.Must(template.New("broker").Parse(brokerTemplateSource))

	outDir := generatedManifestsDir + "/generated"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Println("Error creating output directory:", err)
		os.Exit(1)
	}

	if err := writeTemplateToFile(namespaceTemplate, NamespaceTemplateData{
		Namespace: config.Namespace,
	}, outDir+"/namespace.yaml"); err != nil {
		fmt.Println("Error generating namespace manifest:", err)
		os.Exit(1)
	}

	if err := writeTemplateToFile(brokerTemplate, BrokerTemplateData{
		Namespace: config.Namespace,
		Replicas:  config.Broker.Replicas,
	}, outDir+"/broker.yaml"); err != nil {
		fmt.Println("Error generating broker manifest:", err)
		os.Exit(1)
	}

	for agentName, agentConfig := range config.Agents {
		fmt.Printf("Processing agent: %s\n", agentName)

		if agentConfig.Image == "" {
			fmt.Printf("Error: agent %s has no image set\n", agentName)
			os.Exit(1)
		}

		replicas := agentConfig.Replicas
		if agentConfig.MinReplicas > 0 || agentConfig.MaxReplicas > 0 {
			replicas = agentConfig.MinReplicas
		}
		if replicas <= 0 {
			replicas = 1
		}

		env := make([]EnvVar, 0, len(agentConfig.Env))
		for name, value := range agentConfig.Env {
			env = append(env, EnvVar{Name: name, Value: value})
		}

		agentData := AgentTemplateData{
			Namespace:         config.Namespace,
			AgentName:         k8sName(agentName),
			Image:             agentConfig.Image,
			Replicas:          replicas,
			ConsumeNamespaces: strings.Join(agentConfig.Consumes, ","),
			ProduceNamespaces: strings.Join(agentConfig.Produces, ","),
			Env:               env,
			EnvFromSecrets:    agentConfig.EnvFromSecrets,
		}

		if agentConfig.MinReplicas > 0 || agentConfig.MaxReplicas > 0 {
			lagPerReplica := agentConfig.LagPerReplica
			if lagPerReplica == 0 {
				lagPerReplica = defaultLagPerReplica
			}
			agentData.HPA = &HPATemplateData{
				MinReplicas:                   agentConfig.MinReplicas,
				MaxReplicas:                   agentConfig.MaxReplicas,
				LagPerReplica:                 lagPerReplica,
				ConsumeNamespaces:             agentConfig.Consumes,
				ScaleDownStabilizationSeconds: hpaScaleDownStabilizationSeconds,
			}
		}

		if err := writeTemplateToFile(agentTemplate, agentData, fmt.Sprintf("%s/%s.yaml", outDir, agentData.AgentName)); err != nil {
			fmt.Println("Error generating agent manifest:", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Successfully generated deployment manifests in %s\n", outDir)
}
