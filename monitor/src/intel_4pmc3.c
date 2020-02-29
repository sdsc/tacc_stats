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
#include "intel_pmc3.h"

//! Collect values in counters for cpu
static void intel_4pmc3_collect_cpu(struct stats_type *type, char *cpu)
{
  struct stats *stats = NULL;
  char msr_path[80];
  int msr_fd = -1;
  int pmc = 0;

  stats = get_current_stats(type, cpu);
  if (stats == NULL)
    goto out;

  TRACE("cpu %s\n", cpu);

  snprintf(msr_path, sizeof(msr_path), "/dev/cpu/%s/msr", cpu);
  msr_fd = open(msr_path, O_RDONLY);
  if (msr_fd < 0) {
    ERROR("cannot open `%s': %m\n", msr_path);
    goto out;
  }
  
#define X(k,r...)							\
    ({									\
      uint64_t val = 0;							\
      if (pread(msr_fd, &val, sizeof(val), IA32_##k) < 0)		\
	TRACE("cannot read `%s' (%08X) through `%s': %m\n", #k, IA32_##k, msr_path); \
      else								\
	stats_set(stats, #k, val);					\
    })
    HT_KEYS;
#undef X
    
 out:
  if (msr_fd >= 0)
    close(msr_fd);
}

static void intel_4pmc3_collect(struct stats_type *type)
{
  int i;
  for (i = 0; i < nr_cpus; i++) {
    char cpu[80];
    snprintf(cpu, sizeof(cpu), "%d", i);
    intel_4pmc3_collect_cpu(type, cpu);
  }
}
static int intel_4pmc3_begin(struct stats_type *type)
{
  int nr = 0;
  int i;
  if (n_pmcs == 4)
    for (i = 0; i < nr_cpus; i++) {
      char cpu[80];
      snprintf(cpu, sizeof(cpu), "%d", i);    
      if (intel_pmc3_begin_cpu(cpu) == 0)
	nr++;
    }  
  if (nr == 0) 
    type->st_enabled = 0;
  return nr > 0 ? 0 : -1;
}

//! Definition of stats entry for this type
struct stats_type intel_4pmc3_stats_type = {
  .st_name = "intel_4pmc3",
  .st_begin = &intel_4pmc3_begin,
  .st_collect = &intel_4pmc3_collect,
#define X SCHEMA_DEF
  .st_schema_def = JOIN(HT_KEYS),
#undef X
};
