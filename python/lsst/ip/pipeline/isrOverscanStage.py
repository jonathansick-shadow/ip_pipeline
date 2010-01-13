#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr

class IsrOverscanStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrOverscanStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrOverscanStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing overscan subtraction.")
        
        #grab exposure and overscan bbox from clipboard
        overscanBBox = clipboard.get(self.policy.getString("inputKeys.overscansec"))
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        fittype = clipboard.get(self.policy.getString("inputKeys.overscanfittype"))
        ipIsr.overscanCorrection(exposure, overscanBBox, fittype)

        #output products
        clipboard.put(self.policy.get("outputKeys.overscanCorrectedExposure"), exposure)
        
class IsrOverscanStage(harnessStage.Stage):
    parallelClass = IsrOverscanStageParallel

