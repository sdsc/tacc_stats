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
#include "pscanf.h"
#include "pci.h"
#include "intel_pmc_uncore.h"

#define CTL_KEYS \
  X(CTL0, "C", ""),   \
    X(CTL1, "C", ""), \
    X(CTL2, "C", ""), \
    X(CTL3, "C", "")

#define CTR_KEYS	   \
    X(CTR0, "E,W=48", ""), \
      X(CTR1, "E,W=48", ""),			\
      X(CTR2, "E,W=48", ""),			\
      X(CTR3, "E,W=48", ""),			\
      X(FIXED_CTR,"E,W=48","")

#define KEYS CTL_KEYS, CTR_KEYS

#define PERF_EVENT(event, umask)		\
  ( (event)					\
    | (umask << 8)				\
    | (0UL << 18) /* Edge Detection. */		\
    | (1UL << 22) /* Enable. */			\
    | (0UL << 23) /* Invert */			\
    | (0x01UL << 24) /* Threshold */		\
    )

#define CAS_READS           PERF_EVENT(0x04, 0x03)
#define CAS_WRITES          PERF_EVENT(0x04, 0x0C)
#define ACT_COUNT           PERF_EVENT(0x01, 0x0B)
#define PRE_COUNT_ALL       PERF_EVENT(0x02, 0x03)
#define PRE_COUNT_MISS      PERF_EVENT(0x02, 0x01)

static uint32_t events[] = {
  CAS_READS, CAS_WRITES, ACT_COUNT, PRE_COUNT_MISS,
};
static int dids[] = {0x0eb4, 0x0eb5, 0x0eb0, 0x0eb1}; 

static int intel_ivb_imc_begin(struct stats_type *type)
{
  int nr = 0;
    
  char **dev_paths = NULL;
  int nr_devs;

  if (pci_map_create(&dev_paths, &nr_devs, dids, 4) < 0)
    TRACE("Failed to identify pci devices");
  
  int i;
  for (i = 0; i < nr_devs; i++)
    if (intel_pmc_uncore_begin_dev(dev_paths[i], events, 4) == 0)
      nr++;

  if (nr == 0)
    type->st_enabled = 0;
  pci_map_destroy(&dev_paths, nr_devs);
  return nr > 0 ? 0 : -1;
}

static void intel_ivb_imc_collect(struct stats_type *type)
{
  char **dev_paths = NULL;
  int nr_devs;
  if (pci_map_create(&dev_paths, &nr_devs, dids, 4) < 0)
    TRACE("Failed to identify pci devices");
  
  int i;
  for (i = 0; i < nr_devs; i++)
    intel_pmc_uncore_collect_dev(type, dev_paths[i]);  
  pci_map_destroy(&dev_paths, nr_devs);
}

struct stats_type intel_ivb_imc_stats_type = {
  .st_name = "intel_ivb_imc",
  .st_begin = &intel_ivb_imc_begin,
  .st_collect = &intel_ivb_imc_collect,
#define X SCHEMA_DEF
  .st_schema_def = JOIN(KEYS),
#undef X
};
