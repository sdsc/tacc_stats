#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <malloc.h>
#include <ctype.h>
#include <fcntl.h>
#include "stats.h"
#include "trace.h"
#include "cpuid.h"
#include "intel_pmc3.h"

#define MEM_UNCORE_RETIRED_REMOTE_DRAM PERF_EVENT(0x0F, 0x10) /* CHECKME */
#define MEM_UNCORE_RETIRED_LOCAL_DRAM  PERF_EVENT(0x0F, 0x20) /* CHECKME */
#define FP_COMP_OPS_EXE_X87            PERF_EVENT(0x10, 0x01)
#define MEM_LOAD_RETIRED_L1D_HIT       PERF_EVENT(0xCB, 0x01)
#define MEM_LOAD_RETIRED_L2_HIT        PERF_EVENT(0xCB, 0x02)
#define MEM_LOAD_RETIRED_L3_HIT        PERF_EVENT(0xCB, 0x0C)
#define MEM_LOAD_RETIRED_L3_MISS       PERF_EVENT(0xCB, 0x10) /* May be same as 0x0F/0x10 + 0x0F/0x20. */
#define FP_COMP_OPS_EXE_SSE_FP_PACKED  PERF_EVENT(0x10, 0x10)
#define FP_COMP_OPS_EXE_SSE_FP_SCALAR  PERF_EVENT(0x10, 0x20)

static int intel_nhm_begin(struct stats_type *type)
{
  int n_pmcs = 0;
  int nr = 0;

  uint64_t events[] = {
    MEM_UNCORE_RETIRED_REMOTE_DRAM,
    MEM_UNCORE_RETIRED_LOCAL_DRAM,
    FP_COMP_OPS_EXE_SSE_FP_PACKED,
    FP_COMP_OPS_EXE_SSE_FP_SCALAR
  };

  int i;
  if (signature(NEHALEM, &n_pmcs))
    for (i = 0; i < nr_cpus; i++) {
      char cpu[80];
      snprintf(cpu, sizeof(cpu), "%d", i);
      if (intel_pmc3_begin_cpu(cpu, events, n_pmcs) == 0)
	nr++;
    }

  if (nr == 0) 
    type->st_enabled = 0;

  return nr > 0 ? 0 : -1;
}

static void intel_nhm_collect(struct stats_type *type)
{
  int i;
  for (i = 0; i < nr_cpus; i++) {
    char cpu[80];
    snprintf(cpu, sizeof(cpu), "%d", i);
    intel_pmc3_collect_cpu(type, cpu);
  }
}

struct stats_type intel_nhm_stats_type = {
  .st_name = "intel_nhm",
  .st_begin = &intel_nhm_begin,
  .st_collect = &intel_nhm_collect,
#define X SCHEMA_DEF
  .st_schema_def = JOIN(KEYS),
#undef X
};
