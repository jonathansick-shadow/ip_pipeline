#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom

class IsrSaturationStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrSaturationStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrSaturationStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing Saturation correction.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        exposure = exposure.convertF()
        fwhm = clipboard.get(self.policy.getString("inputKeys.fwhm"))
        ampId = clipboard.get("ampId")
        amp = cameraGeom.cast_Amp(exposure.getDetector())
        saturation = amp.getElectronicParams().getSaturationLevel()
        ipIsr.saturationCorrection(exposure, int(saturation), float(fwhm))

        #output products
        clipboard.put(self.policy.get("outputKeys.saturationCorrectedExposure"),
                exposure)
        
class IsrSaturationStage(harnessStage.Stage):
    parallelClass = IsrSaturationStageParallel

