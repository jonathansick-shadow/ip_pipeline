#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr

class IsrFringeStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrFringeStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrFringeStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing Fringe correction.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        fringeexposure =
        clipboard.get(self.policy.getString("inputKeys.fringeexposure"))
        ipIsr.fringeCorrection(exposure, fringeexposure)

        #output products
        clipboard.put(self.policy.get("outputKeys.FringeCorrectedExposure"), exposure)
        
class IsrFringeStage(harnessStage.Stage):
    parallelClass = IsrFringeStageParallel

