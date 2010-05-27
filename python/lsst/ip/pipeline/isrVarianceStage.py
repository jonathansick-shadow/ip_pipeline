#!/usr/bin/env python
import lsst.pex.harness.stage as harnessStage
import lsst.afw.display.ds9 as ds9


from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr

class IsrVarianceStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrVarianceStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrVarianceStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Calculating variance from image counts.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        ipIsr.updateVariance(exposure)
        #output products
        clipboard.put(self.policy.get("outputKeys.varianceAddedExposure"), exposure)
        
class IsrVarianceStage(harnessStage.Stage):
    parallelClass = IsrVarianceStageParallel

