#!/usr/bin/env python
import math
import os

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy

import lsst.ip.isr as ipIsr

class IsrLinearityStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        #self.log = Log(self.log, "IsrLinearityStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrLinearityStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing linearity correction.")
        linearityPolicy = pexPolicy.Policy()
        lutPolicyPath = clipboard.get(self.policy.getString("inputKeys.lutPolicyPath"))
        lutPolicyFile = clipboard.get(self.policy.getString("inputKeys.lutPolicyFile"))
        lutPolicy = pexPolicy.Policy.createPolicy(os.path.join(lutPolicyPath,lutPolicyFile))
        lut = ipIsr.lookupTableFromPolicy(lutPolicy)
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        ipIsr.linearization(exposure, lut)

        #output products
        clipboard.put(self.policy.get("outputKeys.linearityCorrectedExposure"), exposure)
        
class IsrLinearityStage(harnessStage.Stage):
    parallelClass = IsrLinearityStageParallel
