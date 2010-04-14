#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr

class IsrFlatStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrFlatStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrFlatStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing Flat correction.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        flatexposure = clipboard.get(self.policy.getString("inputKeys.flatexposure"))
        scalingtype = self.policy.getString("parameters.flatScalingType")
        ipIsr.flatCorrection(exposure, flatexposure, scalingtype)

        #output products
        clipboard.put(self.policy.get("outputKeys.flatCorrectedExposure"), exposure)
        
class IsrFlatStage(harnessStage.Stage):
    parallelClass = IsrFlatStageParallel

