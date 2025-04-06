package venom

import (
    "context"
    "fmt"
    "os"
    "runtime/pprof"
    "time"

    "github.com/ovh/cds/sdk/interpolate"
    "github.com/pkg/errors"
    "gopkg.in/yaml.v2" // Added for yaml.Unmarshal
)

func (v *Venom) runTestSuite(ctx context.Context, ts *TestSuite) error {
    if v.Verbose == 3 {
        var filename, filenameCPU, filenameMem string
        if v.OutputDir != "" {
            filename = v.OutputDir + "/"
        }
        filenameCPU = filename + "pprof_cpu_profile_" + ts.Filename + ".prof"
        filenameMem = filename + "pprof_mem_profile_" + ts.Filename + ".prof"
        fCPU, errCPU := os.Create(filenameCPU)
        fMem, errMem := os.Create(filenameMem)
        if errCPU != nil || errMem != nil {
            return fmt.Errorf("error while create profile file CPU:%v MEM:%v", errCPU, errMem)
        } else {
            pprof.StartCPUProfile(fCPU)
            p := pprof.Lookup("heap")
            defer p.WriteTo(fMem, 1)
            defer pprof.StopCPUProfile()
        }
    }

    // Initialize the testsuite variables and compute a first interpolation over them
    ts.Vars.AddAll(v.variables.Clone())
    vars, _ := DumpStringPreserveCase(ts.Vars)
    for k, val := range vars {
        varMap := make(map[string]string)
        for kk, vv := range ts.Vars {
            varMap[kk] = fmt.Sprintf("%v", vv)
        }
        computedV, err := interpolate.Do(fmt.Sprintf("%v", val), varMap)
        if err != nil {
            return errors.Wrapf(err, "error while computing variable %s=%q", k, val)
        }
        ts.Vars.Add(k, computedV)
    }

    exePath, err := os.Executable()
    if err != nil {
        return errors.Wrapf(err, "failed to get executable path")
    } else {
        ts.Vars.Add("venom.executable", exePath)
    }

    ts.Vars.Add("venom.outputdir", v.OutputDir)
    ts.Vars.Add("venom.libdir", v.LibDir)
    ts.Vars.Add("venom.testsuite", ts.Name)
    ts.ComputedVars = H{}

    ctx = context.WithValue(ctx, ContextKey("testsuite"), ts.Name)
    Info(ctx, "Starting testsuite")
    defer Info(ctx, "Ending testsuite")

    totalSteps := 0
    for _, tc := range ts.TestCases {
        totalSteps += len(tc.testSteps)
    }

    ts.Status = StatusRun
    Info(ctx, "With secrets in testsuite")
    for _, secret := range ts.Secrets {
        Info(ctx, "secret  %+v", secret)
    }

    v.runTestCases(ctx, ts)

    var isFailed bool
    var nSkip int
    for _, tc := range ts.TestCases {
        if tc.Status == StatusFail {
            isFailed = true
            ts.NbTestcasesFail++
        } else if tc.Status == StatusSkip {
            nSkip++
            ts.NbTestcasesSkip++
        } else if tc.Status == StatusPass {
            ts.NbTestcasesPass++
        }
    }

    if isFailed {
        ts.Status = StatusFail
        v.Tests.NbTestsuitesFail++
    } else if nSkip > 0 && nSkip == len(ts.TestCases) {
        ts.Status = StatusSkip
        v.Tests.NbTestsuitesSkip++
    } else {
        ts.Status = StatusPass
        v.Tests.NbTestsuitesPass++
    }
    return nil
}

func (v *Venom) runTestCases(ctx context.Context, ts *TestSuite) {
    v.Println(" • %s (%s)", ts.Name, ts.Filepath)

    for i := range ts.TestCases {
        tc := &ts.TestCases[i]
        tc.IsEvaluated = true

        // Handle test case-level range
        if tc.Range != nil {
            rangeItems, err := v.interpolateRange(ctx, tc.Range, tc.Vars)
            if err != nil {
                tc.Status = StatusFail
                tc.Failures = append(tc.Failures, Failure{Value: fmt.Sprintf("Range error: %v", err)})
                v.Print(" \t• %s %s\n", tc.Name, Red(StatusFail))
                continue
            }
            for idx, item := range rangeItems {
                clonedTC := v.cloneTestCase(tc, idx)
                v.injectRangeItem(clonedTC, item, idx)
                v.runTestCase(ctx, ts, clonedTC)
                ts.TestCases = append(ts.TestCases, *clonedTC)
            }
        } else {
            v.runTestCase(ctx, ts, tc)
        }
    }
}

func (v *Venom) cloneTestCase(tc *TestCase, idx int) *TestCase {
    cloned := *tc
    cloned.Name = fmt.Sprintf("%s_%d", tc.Name, idx)
    cloned.originalName = tc.Name
    cloned.Vars = tc.Vars.Clone()
    cloned.TestStepResults = nil
    cloned.Status = ""
    cloned.Start = time.Time{}
    cloned.End = time.Time{}
    cloned.Duration = 0
    cloned.computedVerbose = nil
    return &cloned
}

func (v *Venom) injectRangeItem(tc *TestCase, item interface{}, idx int) {
    switch val := item.(type) {
    case map[string]interface{}:
        for k, v := range val {
            tc.Vars[k] = v
        }
        tc.Vars["index"] = idx
        if key, ok := val["key"]; ok {
            tc.Vars["key"] = key
        }
    case string, int, float64, bool:
        tc.Vars["value"] = val
        tc.Vars["index"] = idx
    }
}

func (v *Venom) interpolateRange(ctx context.Context, r interface{}, vars H) ([]interface{}, error) {
    switch val := r.(type) {
    case string:
        varMap := make(map[string]string)
        for k, v := range vars {
            varMap[k] = fmt.Sprintf("%v", v)
        }
        interpolated, err := interpolate.Do(val, varMap)
        if err != nil {
            return nil, err
        }
        var items []interface{}
        if err := yaml.Unmarshal([]byte(interpolated), &items); err != nil {
            return nil, fmt.Errorf("invalid range string: %v", err)
        }
        return items, nil
    case int:
        var items []interface{}
        for i := 0; i < val; i++ {
            items = append(items, i)
        }
        return items, nil
    case []interface{}, map[string]interface{}:
        return flattenRange(val), nil
    default:
        return nil, fmt.Errorf("unsupported range type: %T", r)
    }
}

func flattenRange(r interface{}) []interface{} {
    switch val := r.(type) {
    case []interface{}:
        return val
    case map[string]interface{}:
        var items []interface{}
        for k, v := range val {
            m := map[string]interface{}{"key": k, "value": v}
            items = append(items, m)
        }
        return items
    default:
        return []interface{}{r}
    }
}

// Parse the suite to find unreplaced and extracted variables
func (v *Venom) parseTestSuite(ts *TestSuite) ([]string, []string, error) {
    return v.parseTestCases(ts)
}

// Parse the test cases to find unreplaced and extracted variables
func (v *Venom) parseTestCases(ts *TestSuite) ([]string, []string, error) {
    var vars []string
    var extractsVars []string
    for i := range ts.TestCases {
        tc := &ts.TestCases[i]
        tc.originalName = tc.Name
        tc.Vars = tc.Vars.Clone()
        tc.Vars.Add("venom.testcase", tc.Name)

        if len(tc.Skipped) == 0 {
            tvars, tExtractedVars, err := v.parseTestCase(ts, tc)
            if err != nil {
                return nil, nil, err
            }
            for _, k := range tvars {
                var found bool
                for i := 0; i < len(vars); i++ {
                    if vars[i] == k {
                        found = true
                        break
                    }
                }
                if !found {
                    vars = append(vars, k)
                }
            }
            for _, k := range tExtractedVars {
                var found bool
                for i := 0; i < len(extractsVars); i++ {
                    if extractsVars[i] == k {
                        found = true
                        break
                    }
                }
                if !found {
                    extractsVars = append(extractsVars, k)
                }
            }
        }
    }
    return vars, extractsVars, nil
}