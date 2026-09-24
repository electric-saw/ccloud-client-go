// Command ccloud-gen is the code-generation toolchain for ccloud-client-go.
//
// Subcommands:
//
//	ccloud-gen preprocess <spec.yaml> <out.yaml>   strip readOnly/example/examples
//	ccloud-gen buildgroups <spec.yaml>             derive build/groups.json + opmap.json
//	ccloud-gen genlines <groups.json>              print `pkg|tag1,tag2` lines
//	ccloud-gen split <groups.json> <opmap.json>    one file per tag per package
//	ccloud-gen facade <groups.json>                render ccloud/client.go from template
//
// Typical flow (see mise.toml `gen` task):
//
//	ccloud-gen preprocess spec/openapi.yaml spec/openapi.norm.yaml
//	ccloud-gen buildgroups spec/openapi.yaml
//	oapi-codegen ... per group
//	ccloud-gen split build/groups.json build/opmap.json
//	ccloud-gen facade build/groups.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

const modulePath = "github.com/electric-saw/ccloud-client-go/v2"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	args := os.Args[2:]
	var err error
	switch os.Args[1] {
	case "preprocess":
		err = cmdPreprocess(args)
	case "buildgroups":
		err = cmdBuildgroups(args)
	case "genlines":
		err = cmdGenlines(args)
	case "split":
		err = cmdSplit(args)
	case "facade":
		err = cmdFacade(args)
	case "gen":
		err = cmdGen(args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "ccloud-gen: unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "ccloud-gen: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: ccloud-gen <subcommand> [args]

subcommands:
  gen [spec.yaml]                      run the full pipeline (default: spec/openapi.yaml)
  preprocess <spec.yaml> <out.yaml>   strip readOnly/example/examples keys
  buildgroups <spec.yaml>             write build/groups.json + build/opmap.json
  genlines <groups.json>              print `+"`pkg|tag1,tag2`"+` lines
  split <groups.json> <opmap.json>    split client.gen.go into one file per tag
  facade <groups.json>                render ccloud/client.go from client.go.tmpl
`)
}

// ---------------------------------------------------------------------------
// gen (orchestrator)
// ---------------------------------------------------------------------------

// cmdGen runs the full generation pipeline:
//
//	preprocess -> buildgroups -> oapi-codegen (per group) -> split -> facade -> goimports -> gofmt
func cmdGen(args []string) error {
	spec := "spec/openapi.yaml"
	if len(args) == 1 {
		spec = args[0]
	}
	if len(args) > 1 {
		return fmt.Errorf("usage: ccloud-gen gen [spec.yaml]")
	}

	steps := []struct {
		name string
		fn   func() error
	}{
		{"preprocess", func() error { return cmdPreprocess([]string{spec, "spec/openapi.norm.yaml"}) }},
		{"buildgroups", func() error { return cmdBuildgroups([]string{spec}) }},
		{"oapi-codegen", cmdOapiCodegen},
		{"split", func() error { return cmdSplit([]string{"build/groups.json", "build/opmap.json"}) }},
		{"unexport", cmdUnexport},
		{"facade", func() error { return cmdFacade([]string{"build/groups.json"}) }},
		{"goimports", cmdGoimports},
		{"gofmt", cmdGofmt},
	}
	for _, s := range steps {
		fmt.Printf("[gen] %s\n", s.name)
		if err := s.fn(); err != nil {
			return fmt.Errorf("%s: %w", s.name, err)
		}
	}
	return nil
}

// cmdOapiCodegen runs the oapi-codegen binary once per group, emitting
// ccloud/<pkg>/client.gen.go from the normalized spec.
//
// The plain client is generated as unexported `oasClient` (client-type-name)
// so the only exported client surface per package is ClientWithResponses.
func cmdOapiCodegen() error {
	lines, err := runOutput("go", "run", "./scripts/ccloud-gen", "genlines", "build/groups.json")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSpace(lines), "\n") {
		if line == "" {
			continue
		}
		pkg, tags, ok := strings.Cut(line, "|")
		if !ok || pkg == "" {
			continue
		}
		dir := filepath.Join("ccloud", pkg)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		out := filepath.Join(dir, "client.gen.go")
		fmt.Printf("  gen: %s -> %s\n", tags, out)

		// oapi-codegen v2.8: client-type-name is only settable via config file.
		cfg := filepath.Join(dir, ".oapi-gen.yaml")
		cfgContent := fmt.Sprintf(`package: %s
output: %s
generate:
  client: true
  models: true
output-options:
  client-type-name: oasClient
  include-tags:
%s
`, pkg, out, tagListYAML(tags))
		if err := os.WriteFile(cfg, []byte(cfgContent), 0o644); err != nil {
			return err
		}
		if err := run("oapi-codegen", "--config", cfg, "spec/openapi.norm.yaml"); err != nil {
			return err
		}
		if err := os.Remove(cfg); err != nil {
			return err
		}
	}
	return nil
}

// tagListYAML renders a []string YAML block for the config's include-tags.
func tagListYAML(tags string) string {
	parts := strings.Split(tags, ",")
	lines := make([]string, 0, len(parts))
	for _, t := range parts {
		lines = append(lines, "    - "+strconv.Quote(t))
	}
	return strings.Join(lines, "\n")
}

// removeInterfaceBlock removes a `type NAME interface { ... }` declaration
// (plus any preceding comment lines) from the source text.
func removeInterfaceBlock(text, name string) string {
	lines := strings.Split(text, "\n")
	var out []string
	inBlock := false
	for i := 0; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if !inBlock && strings.HasPrefix(trimmed, "type "+name+" interface") {
			inBlock = true
			// drop preceding comment lines (backwards)
			for len(out) > 0 {
				last := strings.TrimSpace(out[len(out)-1])
				if strings.HasPrefix(last, "//") || last == "" {
					out = out[:len(out)-1]
				} else {
					break
				}
			}
			continue
		}
		if inBlock {
			if trimmed == "}" {
				inBlock = false
			}
			continue
		}
		out = append(out, lines[i])
	}
	return strings.Join(out, "\n")
}

// cmdGoimports runs goimports -w over every generated .gen.go file.
func cmdGoimports() error {
	return run("sh", "-c", `find ccloud -name '*.gen.go' -exec goimports -w {} +`)
}

// cmdUnexport hides the plain http.Response methods from the public API.
//
// oapi-codegen always emits a plain client (now unexported `oasClient`) whose
// methods return *http.Response, plus a `ClientInterface` that promotes those
// methods into ClientWithResponses' public surface. Since the WithResponse
// wrappers delegate to the plain methods, the plain methods must exist — but
// they can be unexported (lowercase) so nothing exposes *http.Response.
//
// Renames, per generated package:
//   - ClientInterface              -> clientInterface
//   - ClientWithResponsesInterface  -> clientWithResponsesInterface
//   - func (c *oasClient) Foo(...)  -> func (c *oasClient) foo(...)   (plain methods)
//   - Foo(ctx ...) inside clientInterface -> foo(ctx ...)             (interface decls)
//   - c.Foo(...)  in wrappers       -> c.foo(...)                     (their calls)
func cmdUnexport() error {
	files, err := filepath.Glob(filepath.Join("ccloud", "*", "*.gen.go"))
	if err != nil {
		return err
	}
	for _, file := range files {
		src, err := os.ReadFile(file)
		if err != nil {
			return err
		}
		text := string(src)
		orig := text

		// interfaces: unexport the type names (declaration AND embedding reference)
		text = strings.ReplaceAll(text, "type ClientInterface interface", "type clientInterface interface")
		text = strings.ReplaceAll(text, "ClientInterface", "clientInterface")
		text = strings.ReplaceAll(text, "type ClientWithResponsesInterface interface", "type clientWithResponsesInterface interface")
		// remove the (unused) clientWithResponsesInterface block entirely —
		// it's generated but never referenced, and it's not exported.
		text = removeInterfaceBlock(text, "clientWithResponsesInterface")

		// plain method definitions on oasClient: Foo( -> foo(
		text = unexportPlainMethods(text)

		// interface method declarations: inside clientInterface, Foo(ctx -> foo(ctx
		text = unexportInterfaceMethods(text)

		if text != orig {
			if err := os.WriteFile(file, []byte(text), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// unexportPlainMethods lowercases the first letter of plain client methods:
//
//	func (c *oasClient) ListIamV2ApiKeys( -> func (c *oasClient) listIamV2ApiKeys(
//	c.ListIamV2ApiKeys(                     -> c.listIamV2ApiKeys(   (wrapper delegations)
//
// It must NOT touch the exported WithResponse wrappers (c.XxxWithResponse()
// are wrapper methods — they keep their exported names).
func unexportPlainMethods(text string) string {
	// 1) definitions on oasClient
	reDef := regexp.MustCompile(`func \(c \*oasClient\) ([A-Z]\w*)\(`)
	text = reDef.ReplaceAllStringFunc(text, func(m string) string {
		sub := reDef.FindStringSubmatch(m)
		return "func (c *oasClient) " + strings.ToLower(sub[1][:1]) + sub[1][1:] + "("
	})
	// 2) calls in wrappers: c.Foo( -> c.foo(  (including WithBody variants,
	// which are plain methods; only WithResponse names are wrapper methods
	// themselves and never called via c.)
	reCall := regexp.MustCompile(`\bc\.([A-Z]\w*)\(`)
	text = reCall.ReplaceAllStringFunc(text, func(m string) string {
		sub := reCall.FindStringSubmatch(m)
		name := sub[1]
		if strings.HasSuffix(name, "WithResponse") {
			return m
		}
		return "c." + strings.ToLower(name[:1]) + name[1:] + "("
	})
	return text
}

// unexportInterfaceMethods lowercases the plain-method declarations inside the
// clientInterface block (they are promoted into ClientWithResponses; the
// WithResponse wrapper methods in clientWithResponsesInterface stay exported).
func unexportInterfaceMethods(text string) string {
	var out strings.Builder
	inClientIface := false
	for _, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "type clientInterface interface") {
			inClientIface = true
			out.WriteString(line + "\n")
			continue
		}
		if inClientIface && (strings.HasPrefix(trimmed, "type ") || trimmed == "}") {
			// leaving the interface block (next type decl or closing brace)
			if trimmed == "}" {
				out.WriteString(line + "\n")
				inClientIface = false
				continue
			}
			inClientIface = false
		}
		if inClientIface {
			// Foo(ctx ...) -> foo(ctx ...)  (method declaration inside interface)
			// WithBody methods are ALSO plain (return *http.Response) — lowercase them.
			// Only WithResponse wrappers stay exported (but those live in the
			// clientWithResponsesInterface, not here).
			m := regexp.MustCompile(`^\t([A-Z]\w*)\(`)
			sub := m.FindStringSubmatch(line)
			if sub != nil && !strings.HasSuffix(sub[1], "WithResponse") {
				line = strings.Replace(line, sub[1], strings.ToLower(sub[1][:1])+sub[1][1:], 1)
			}
		}
		out.WriteString(line + "\n")
	}
	return out.String()
}

// cmdGofmt formats ccloud/client.go and fails if any generated file is
// left unformatted.
func cmdGofmt() error {
	if err := run("gofmt", "-w", "ccloud/client.go"); err != nil {
		return err
	}
	out, err := runOutput("gofmt", "-l", "ccloud")
	if err != nil {
		return err
	}
	if strings.TrimSpace(out) != "" {
		return fmt.Errorf("gofmt issues:\n%s", out)
	}
	return nil
}

// run executes a command, streaming output, failing on error.
func run(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// runOutput executes a command and returns its stdout.
func runOutput(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	return string(out), err
}

// ---------------------------------------------------------------------------
// preprocess
// ---------------------------------------------------------------------------

func cmdPreprocess(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: ccloud-gen preprocess <spec.yaml> <out.yaml>")
	}
	src, dst := args[0], args[1]

	in, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(in, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", src, err)
	}

	keys := map[string]bool{"readOnly": true, "example": true, "examples": true}
	removed := map[string]int{}
	stripKeys(&doc, keys, removed)

	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	if err := os.WriteFile(dst, out, 0o644); err != nil {
		return err
	}
	parts := ""
	for _, k := range []string{"readOnly", "example", "examples"} {
		parts += fmt.Sprintf(" %s=%d", k, removed[k])
	}
	fmt.Println("preprocess:" + parts)
	return nil
}

func stripKeys(node *yaml.Node, keys map[string]bool, removed map[string]int) {
	if node == nil {
		return
	}
	switch node.Kind {
	case yaml.DocumentNode:
		// recurse into the single root child
		for _, child := range node.Content {
			stripKeys(child, keys, removed)
		}
	case yaml.MappingNode:
		content := node.Content
		i := 0
		for i+1 < len(content) {
			if keys[content[i].Value] {
				removed[content[i].Value]++
				content = append(content[:i], content[i+2:]...)
				continue
			}
			stripKeys(content[i+1], keys, removed)
			i += 2
		}
		node.Content = content
	case yaml.SequenceNode:
		for _, item := range node.Content {
			stripKeys(item, keys, removed)
		}
	case yaml.AliasNode:
		stripKeys(node.Alias, keys, removed)
	}
}

// ---------------------------------------------------------------------------
// buildgroups
// ---------------------------------------------------------------------------

// Tags forced into a specific package regardless of their path namespace.
var buildOverrides = map[string]string{
	"Compute Pools (fcpm/v2)":       "flink",
	"Statements (sql/v1)":           "flink",
	"Flink Artifacts (artifact/v1)": "flink",
	"Presigned Urls (artifact/v1)":  "flink",
}

var pkgRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitizePkg(s string) string { return pkgRe.ReplaceAllString(s, "_") }

func cmdBuildgroups(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ccloud-gen buildgroups <spec.yaml>")
	}
	src := args[0]

	in, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(in, &doc); err != nil {
		return fmt.Errorf("parse %s: %w", src, err)
	}
	root := doc.Content[0]

	tagNS := map[string]string{}
	opMap := map[string]string{}
	paths := mapNode(root, "paths")
	if paths != nil && paths.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(paths.Content); i += 2 {
			path := paths.Content[i].Value
			methods := paths.Content[i+1]
			if methods.Kind != yaml.MappingNode {
				continue
			}
			for j := 0; j+1 < len(methods.Content); j += 2 {
				method := methods.Content[j].Value
				switch method {
				case "get", "post", "put", "delete", "patch":
				default:
					continue
				}
				op := methods.Content[j+1]
				if op.Kind != yaml.MappingNode {
					continue
				}
				var tag, opID string
				for k := 0; k+1 < len(op.Content); k += 2 {
					switch op.Content[k].Value {
					case "tags":
						if op.Content[k+1].Kind == yaml.SequenceNode && len(op.Content[k+1].Content) > 0 {
							tag = op.Content[k+1].Content[0].Value
						}
					case "operationId":
						opID = op.Content[k+1].Value
					}
				}
				if opID == "" {
					continue
				}
				if tag == "" {
					tag = "<untagged>"
				}
				opMap[opID] = tag
				ns := strings.Split(strings.TrimPrefix(path, "/"), "/")[0]
				if prev, ok := tagNS[tag]; !ok || ns < prev {
					tagNS[tag] = ns
				}
			}
		}
	}

	groups := map[string][]string{}
	for tag, ns := range tagNS {
		pkg := buildOverrides[tag]
		if pkg == "" {
			pkg = ns
		}
		pkg = sanitizePkg(pkg)
		groups[pkg] = append(groups[pkg], tag)
	}
	for pkg := range groups {
		sort.Strings(groups[pkg])
	}

	if err := os.MkdirAll("build", 0o755); err != nil {
		return err
	}
	g, _ := json.MarshalIndent(groups, "", "  ")
	if err := os.WriteFile(filepath.Join("build", "groups.json"), append(g, '\n'), 0o644); err != nil {
		return err
	}
	o, _ := json.MarshalIndent(opMap, "", "  ")
	if err := os.WriteFile(filepath.Join("build", "opmap.json"), append(o, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("buildgroups: %d groups, %d operations\n", len(groups), len(opMap))
	return nil
}

func mapNode(root *yaml.Node, key string) *yaml.Node {
	if root == nil || root.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == key {
			return root.Content[i+1]
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// genlines
// ---------------------------------------------------------------------------

func cmdGenlines(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ccloud-gen genlines <groups.json>")
	}
	var groups map[string][]string
	if err := readJSON(args[0], &groups); err != nil {
		return err
	}
	pkgs := make([]string, 0, len(groups))
	for pkg := range groups {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		fmt.Printf("%s|%s\n", pkg, strings.Join(groups[pkg], ","))
	}
	return nil
}

// ---------------------------------------------------------------------------
// split
// ---------------------------------------------------------------------------

var (
	splitOps      map[string]string // oid -> tag
	splitOidsByLn []string          // oids sorted by method-name length desc
)

func cmdSplit(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("usage: ccloud-gen split <groups.json> <opmap.json>")
	}
	var groups map[string][]string
	if err := readJSON(args[0], &groups); err != nil {
		return err
	}
	var opmap map[string]string
	if err := readJSON(args[1], &opmap); err != nil {
		return err
	}
	splitOps = make(map[string]string, len(opmap))
	for oid, tag := range opmap {
		splitOps[oid] = tag
	}
	splitOidsByLn = make([]string, 0, len(splitOps))
	for oid := range splitOps {
		splitOidsByLn = append(splitOidsByLn, oid)
	}
	sort.Slice(splitOidsByLn, func(i, j int) bool {
		return len(methodName(splitOidsByLn[i])) > len(methodName(splitOidsByLn[j]))
	})

	// Sanitize group keys to Go package names (dir names): hyphen -> underscore.
	for pkg, tags := range groups {
		clean := sanitizePkg(pkg)
		if clean != pkg {
			fmt.Printf("note: group key %q -> %q\n", pkg, clean)
			groups[clean] = tags
			delete(groups, pkg)
		}
	}

	for pkg, tags := range groups {
		dir := filepath.Join("ccloud", pkg)
		clientFile := filepath.Join(dir, "client.gen.go")
		if _, err := os.Stat(clientFile); err != nil {
			fmt.Printf("skip %s: no client.gen.go\n", pkg)
			continue
		}
		if err := splitPackage(dir, pkg, tags); err != nil {
			return err
		}
	}
	return nil
}

func methodName(oid string) string {
	if oid == "" {
		return ""
	}
	return strings.ToUpper(oid[:1]) + oid[1:]
}

// opIDFromName maps a generated decl name back to an operationId, or "".
// Longest method-name match wins: e.g. GetSchemaOnly_1 must match
// getSchemaOnly_1, not getSchemaOnly (prefix of it).
func opIDFromName(name string) string {
	for _, oid := range splitOidsByLn {
		mn := methodName(oid)
		if name == mn || name == mn+"WithBody" {
			return oid
		}
		re := regexp.MustCompile("^New" + regexp.QuoteMeta(mn) + "Request(?:WithBody)?$")
		if re.MatchString(name) || name == "Parse"+mn+"Response" {
			return oid
		}
		if strings.HasPrefix(name, mn) {
			re2 := regexp.MustCompile(`(JSONRequestBody|JSONBody|Params|Response)`)
			if re2.MatchString(name) {
				return oid
			}
		}
	}
	return ""
}

func declName(d ast.Decl) string {
	switch x := d.(type) {
	case *ast.FuncDecl:
		return x.Name.Name
	case *ast.GenDecl:
		for _, s := range x.Specs {
			if ts, ok := s.(*ast.TypeSpec); ok {
				return ts.Name.Name
			}
		}
	}
	return ""
}

func splitPackage(dir, pkg string, tags []string) error {
	clientFile := filepath.Join(dir, "client.gen.go")
	src, err := os.ReadFile(clientFile)
	if err != nil {
		return err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, clientFile, src, parser.ParseComments)
	if err != nil {
		return err
	}

	included := map[string]bool{}
	for _, t := range tags {
		included[t] = true
	}

	tagChunks := map[string][]string{}
	shared := []string{}
	for _, d := range f.Decls {
		name := declName(d)
		start := fset.Position(d.Pos()).Offset
		end := fset.Position(d.End()).Offset
		chunk := string(src[start:end])
		if oid := opIDFromName(name); oid != "" && included[splitOps[oid]] {
			tag := splitOps[oid]
			tagChunks[tag] = append(tagChunks[tag], chunk)
		} else {
			shared = append(shared, chunk)
		}
	}

	// rewrite client.gen.go with shared decls only (keep header + package)
	var header strings.Builder
	foundPkg := false
	for _, l := range strings.Split(string(src), "\n") {
		if strings.HasPrefix(l, "package ") {
			header.WriteString(l + "\n\n")
			foundPkg = true
			break
		}
	}
	if !foundPkg {
		return fmt.Errorf("no package declaration found in %s", clientFile)
	}
	newClient := header.String() + strings.Join(shared, "\n") + "\n"
	if err := os.WriteFile(clientFile, []byte(newClient), 0o644); err != nil {
		return err
	}
	fmt.Printf("  %s/client.gen.go (shared, %d decls)\n", pkg, len(shared))

	for _, tag := range tags {
		chunks := tagChunks[tag]
		if len(chunks) == 0 {
			continue
		}
		slug := slugify(tag)
		content := "// Code generated by oapi-codegen + splitter. DO NOT EDIT.\n// Tag: " + tag + "\n\npackage " + pkg + "\n\n" + strings.Join(chunks, "\n") + "\n"
		if err := os.WriteFile(filepath.Join(dir, slug+".gen.go"), []byte(content), 0o644); err != nil {
			return err
		}
		fmt.Printf("  %s/%s.gen.go (%d decls)\n", pkg, slug, len(chunks))
	}
	return nil
}

func slugify(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

// ---------------------------------------------------------------------------
// facade
// ---------------------------------------------------------------------------

type facadePackage struct {
	Pkg        string // go package name, e.g. iam
	Field      string // exported field name, e.g. Iam
	ImportPath string // full import path
}

func cmdFacade(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: ccloud-gen facade <groups.json>")
	}
	var groups map[string][]string
	if err := readJSON(args[0], &groups); err != nil {
		return err
	}

	pkgs := make([]string, 0, len(groups))
	for pkg := range groups {
		pkgs = append(pkgs, sanitizePkg(pkg))
	}
	sort.Strings(pkgs)

	packages := make([]facadePackage, 0, len(pkgs))
	for _, pkg := range pkgs {
		packages = append(packages, facadePackage{
			Pkg:        pkg,
			Field:      exportName(pkg),
			ImportPath: modulePath + "/ccloud/" + pkg,
		})
	}

	tmpl, err := template.ParseFiles(filepath.Join("scripts", "ccloud-gen", "client.go.tmpl"))
	if err != nil {
		return err
	}

	var out strings.Builder
	if err := tmpl.Execute(&out, packages); err != nil {
		return err
	}

	// gofmt the output so the facade is always correctly formatted
	// regardless of template whitespace (e.g. struct field alignment).
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		return err
	}

	outFile := filepath.Join("ccloud", "client.go")
	if err := os.WriteFile(outFile, formatted, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %s (%d packages)\n", outFile, len(packages))
	return nil
}

func exportName(pkg string) string {
	var b strings.Builder
	for _, p := range strings.Split(pkg, "_") {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]) + p[1:])
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// shared
// ---------------------------------------------------------------------------

func readJSON(path string, v any) error {
	in, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(in, v)
}

// keep flag import used (facade-style flag parsing available to future subcommands)
var _ = flag.String
