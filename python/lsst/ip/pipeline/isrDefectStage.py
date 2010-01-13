#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr

class IsrDefectStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrDefectStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrDefectStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing Defect correction.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        defectList = clipboard.get(self.policy.getString("inputKeys.defectList"))
        fwhm = clipboard.get(self.policy.getString("inputKeys.fwhm"))
        #
        # The defects file is in amp coordinates, and we need to
        # shift to the CCD frame
        #
        dx, dy = exposure.getMaskedImage().getXY0()
        for defect in defectList:
            defect.shift(dx, dy)
        ipIsr.maskBadPixelsDef(exposure, defectList, fwhm)

        #output products
        clipboard.put(self.policy.get("outputKeys.defectCorrectedExposure"), exposure)
        
class IsrDefectStage(harnessStage.Stage):
    parallelClass = IsrDefectStageParallel

