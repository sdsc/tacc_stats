from exams import Test

class Load_L1Hits(Test):
  k1 = { 'intel_snb' : ['intel_snb'],
         'intel_hsw' : ['intel_hsw']      
        }
  k2 = {'intel_snb' : ['LOAD_OPS_L1_HIT'],
        'intel_hsw' : ['LOAD_OPS_L1_HIT']
        }
  comp_operator = '>'

  def compute_metric(self):
    self.metric = self.arc(self.ts.data[0])
