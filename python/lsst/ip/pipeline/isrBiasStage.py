#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr

class IsrBiasStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrBiasStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrBiasStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing bias subtraction.")
        
        #grab exposure and bias from clipboard
        biasexposure =
        clipboard.get(self.policy.getString("inputKeys.biasexposure"))
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        ipIsr.biasCorrection(exposure, biasexposure)
        #output products
        clipboard.put(self.policy.get("outputKeys.biasSubtractedExposure"), exposure)
        
class IsrBiasStage(harnessStage.Stage):
    parallelClass = IsrBiasStageParallel

