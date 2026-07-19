#!/usr/bin/env bash
set -euo pipefail

# Repeat one engine_bench command and save the raw output from each run.
# This script records context for the benchmark session; it does not decide
# which workload or rate is correct.
# The saved files are meant to be copied into a later report or parsed by a
# separate analysis script.

usage() {
  cat >&2 <<'EOF'
usage: run_engine_bench.sh --runs N --out DIR -- <engine_bench command...>

example:
  bench/run_engine_bench.sh --runs 5 --out results/mixed -- \
    build/release/bench/engine_bench 1000000 100000 7 \
    --warmup 100000 --cores 2,3,4 --input /tmp/mixed.commands
EOF
  exit 2
}

runs=0
out_dir=

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      [[ $# -ge 2 ]] || usage
      runs=$2
      shift 2
      ;;
    --out)
      [[ $# -ge 2 ]] || usage
      out_dir=$2
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      usage
      ;;
  esac
done

[[ $runs =~ ^[1-9][0-9]*$ ]] || usage
[[ -n $out_dir ]] || usage
[[ $# -gt 0 ]] || usage

cores=
# Find --cores in the benchmark command so the environment file can explain the
# exact CPUs used by producer, matcher, and logger.
for ((i = 1; i <= $#; ++i)); do
  arg=${!i}
  if [[ $arg == --cores ]]; then
    next=$((i + 1))
    [[ $next -le $# ]] && cores=${!next}
  elif [[ $arg == --cores=* ]]; then
    cores=${arg#--cores=}
  fi
done

mkdir -p "$out_dir"

# Keep the exact command next to the results so the run can be repeated later.
command_file="$out_dir/command.txt"
environment_file="$out_dir/environment.txt"
summary_file="$out_dir/summary.txt"

printf '%q ' "$@" >"$command_file"
printf '\n' >>"$command_file"

{
  # These fields make the raw numbers interpretable after the VM is gone.
  echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "git_commit=$(git rev-parse HEAD 2>/dev/null || echo unavailable)"
  echo "kernel=$(uname -a)"
  echo "compiler=$("${CXX:-c++}" --version 2>/dev/null | head -n 1 || echo unavailable)"
  echo
  echo "[cpu]"
  lscpu 2>/dev/null || echo "lscpu unavailable"
  echo
  echo "[governor]"
  # Performance runs should use a fixed CPU governor when the VM exposes one.
  if compgen -G "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor" >/dev/null; then
    for governor in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
      echo "$governor=$(cat "$governor")"
    done
  else
    echo "cpufreq governor files unavailable"
  fi
  echo
  echo "[turbo]"
  # Turbo state affects repeatability, so record it even though this script does
  # not try to change it.
  if [[ -r /sys/devices/system/cpu/intel_pstate/no_turbo ]]; then
    echo "intel_pstate/no_turbo=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)"
  elif [[ -r /sys/devices/system/cpu/cpufreq/boost ]]; then
    echo "cpufreq/boost=$(cat /sys/devices/system/cpu/cpufreq/boost)"
  else
    echo "turbo state file unavailable"
  fi
  echo
  echo "[selected_cores]"
  if [[ -n $cores ]]; then
    echo "cores=$cores"
    IFS=',' read -r -a core_list <<<"$cores"
    selected_sibling_sets=
    selected_nodes=
    for core in "${core_list[@]}"; do
      echo "cpu$core"
      # SMT siblings share execution resources. For clean numbers, selected
      # benchmark cores should not be sibling threads of the same physical core.
      siblings=/sys/devices/system/cpu/cpu$core/topology/thread_siblings_list
      node_glob=(/sys/devices/system/cpu/cpu"$core"/node*)
      if [[ -r $siblings ]]; then
        sibling_set=$(cat "$siblings")
        echo "  thread_siblings=$sibling_set"
        selected_sibling_sets+="$sibling_set"$'\n'
      else
        echo "  thread_siblings=unavailable"
      fi
      if [[ -e ${node_glob[0]} ]]; then
        node_name=$(basename "${node_glob[0]}")
        echo "  numa_node=$node_name"
        selected_nodes+="$node_name"$'\n'
      else
        echo "  numa_node=unavailable"
      fi
    done
    echo
    echo "[selected_core_checks]"
    # These checks are simple warnings captured in the environment file; they
    # help spot a bad core choice before treating the numbers as meaningful.
    unique_sibling_sets=$(printf '%s' "$selected_sibling_sets" | sed '/^$/d' | sort -u | wc -l)
    unique_nodes=$(printf '%s' "$selected_nodes" | sed '/^$/d' | sort -u | wc -l)
    if [[ $unique_sibling_sets -eq ${#core_list[@]} ]]; then
      echo "selected_cores_share_smt_sibling=no"
    else
      echo "selected_cores_share_smt_sibling=yes"
    fi
    if [[ $unique_nodes -eq 1 ]]; then
      echo "selected_cores_single_numa_node=yes"
    else
      echo "selected_cores_single_numa_node=no"
    fi
    echo "non-selected SMT siblings must be kept idle by VM setup if SMT is enabled"
  else
    echo "no --cores argument found in benchmark command"
  fi
} >"$environment_file"

{
  echo "runs=$runs"
  echo "out_dir=$out_dir"
  echo "command=$(cat "$command_file")"
  echo "cores=${cores:-not specified}"
} >"$summary_file"

# Each run gets its own file. If a run fails, the file still contains the exit
# code and timestamps so the failed session is easy to diagnose.
for run in $(seq 1 "$runs"); do
  run_file=$(printf '%s/run_%02d.txt' "$out_dir" "$run")
  echo "run $run/$runs -> $run_file"
  set +e
  {
    echo "run=$run"
    echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    "$@"
    rc=$?
    echo "exit_code=$rc"
    echo "finished_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$run_file"
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "run $run failed with exit code $rc" >&2
    exit "$rc"
  fi
done

echo "saved benchmark session to $out_dir"
