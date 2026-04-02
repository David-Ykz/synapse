package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Models map[string]ModelConfig `yaml:"models"`
	Agents map[string]AgentConfig `yaml:"agents"`
}

type ModelConfig struct {
	Name string `yaml:"name"`
}

type AgentConfig struct {
	InputNamespace  string `yaml:"input_namespace"`
	OutputNamespace string `yaml:"output_namespace"`
	NumReplicas     int    `yaml:"replicas"`
	Model           string `yaml:"model"`
	HandlerUrl      string `yaml:"handler_url"`
}

type AgentTemplateData struct {
	AgentName       string
	NumReplicas     int
	InputNamespace  string
	OutputNamespace string
	ModelName       string
	ToolConfigDir   string
	HandlerUrl      string
	Configs         map[string]string
}

type HandlerTemplateData struct {
	HandlerName string
	HandlerPort string
}

func loadToolConfigs(dirPath string) map[string]string {
	configs := make(map[string]string)

	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("Warning: could not read directory %s: %w\n", dirPath, err)
		return configs
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			path := filepath.Join(dirPath, file.Name())
			content, err := os.ReadFile(path)
			if err != nil {
				fmt.Printf("Error reading file %s: %w\n", path, err)
				continue
			}
			configs[file.Name()] = string(content)
		}
	}
	return configs
}

func writeTemplateToFile(tmpl *template.Template, data interface{}, filepath string) {
	var buf bytes.Buffer
	err := tmpl.Execute(&buf, data)
	if err != nil {
		fmt.Printf("Error filling out template for %s: %w", filepath, err)
		return
	}
	err = os.WriteFile(filepath, buf.Bytes(), 0644)
	if err != nil {
		fmt.Printf("Error writing to file %s: %w\n", filepath, err)
	}
}

func main() {
	// load cli arguments
	if len(os.Args) < 4 {
		fmt.Println("Too few arguments. Required: /path/to/workflow.yaml, /path/to/tools_dir/, /path/to/generated_manifests_dir/")
		return
	}

	workflowPath := os.Args[1]
	toolsDir := os.Args[2]
	generatedManifestsDir := os.Args[3]

	// read workflow.yaml
	workflowYamlFile, err := os.ReadFile(workflowPath)
	if err != nil {
		fmt.Println("Error reading workflow.yaml file:", err)
		return
	}

	var config Config
	err = yaml.Unmarshal(workflowYamlFile, &config)
	if err != nil {
		fmt.Println("Error parsing yaml:", err)
		return
	}

	// initialize template functions
	funcMap := template.FuncMap{
		"indent": func(spaces int, v string) string {
			indent := strings.Repeat(" ", spaces)
			indented := indent + strings.ReplaceAll(v, "\n", "\n"+indent)
			return strings.TrimRight(indented, " ")
		},
	}

	// load templates
	agentWorkerBaseTemplate, err := os.ReadFile("templates/agent-worker.yaml")
	if err != nil {
		fmt.Println("Error reading agent-worker.yaml template:", err)
		return
	}
	handlerBaseTemplate, err := os.ReadFile("templates/handler.yaml")
	if err != nil {
		fmt.Println("Error reading handler.yaml template:", err)
		return
	}

	// parse templates
	agentWorkerTemplate := template.Must(template.New("agent").Funcs(funcMap).Parse(string(agentWorkerBaseTemplate)))
	handlerTemplate := template.Must(template.New("handler").Funcs(funcMap).Parse(string(handlerBaseTemplate)))

	os.MkdirAll(generatedManifestsDir, 0755)

	for agentName, agentConfig := range config.Agents {
		fmt.Printf("Processing agent: %s\n", agentName)

		// parse model name
		actualModelName := ""
		modelInfo, ok := config.Models[agentConfig.Model]
		if ok {
			actualModelName = modelInfo.Name
		} else {
			fmt.Printf("Warning: model %s not found for agent %s", agentConfig.Model, agentName)
		}

		// read all JSON files from the tools directory (if provided)
		configs := loadToolConfigs(toolsDir + "/" + agentName)

		// parse handler URL to get endpoint and port
		handlerParts := strings.Split(agentConfig.HandlerUrl, ":")
		handlerName := handlerParts[0]
		handlerPort := "8000"
		if len(handlerParts) > 1 {
			handlerPort = handlerParts[1]
		} else {
			fmt.Printf("Warning: no port found for handler %s, defaulting to %s", agentConfig.HandlerUrl, handlerPort)
		}

		agentData := AgentTemplateData{
			AgentName:       strings.ReplaceAll(agentName, "_", "-"),
			NumReplicas:     agentConfig.NumReplicas,
			InputNamespace:  agentConfig.InputNamespace,
			OutputNamespace: agentConfig.OutputNamespace,
			ModelName:       actualModelName,
			ToolConfigDir:   "/etc/configs",
			HandlerUrl:      agentConfig.HandlerUrl,
			Configs:         configs,
		}

		handlerData := HandlerTemplateData{
			HandlerName: strings.ReplaceAll(handlerName, "_", "-"),
			HandlerPort: handlerPort,
		}

		// write templates
		writeTemplateToFile(agentWorkerTemplate, agentData, fmt.Sprintf("%s/generated/%s-worker.yaml", generatedManifestsDir, agentData.AgentName))
		writeTemplateToFile(handlerTemplate, handlerData, fmt.Sprintf("%s/generated/%s-handler.yaml", generatedManifestsDir, agentData.AgentName))
	}

	fmt.Printf("Successfully generated deployment manifests in %s/generated\n", generatedManifestsDir)
}
