#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom

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
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
	fittype = self.policy.getString("parameters.overscanFitType")
	amp = cameraGeom.cast_Amp(exposure.getDetector())
        overscanBbox = amp.getDiskBiasSec()
        dataBbox = amp.getDiskDataSec()
        #It just so happens that this is an o.k. place to put the SDQA
        #calculation because the ratings requested at the moment can all be
        #calculated here.  If, for example, an Amp rating an the flat fielded
        #amp were requested, it would have to be calculated separately.
        ipIsr.calculateSdqaAmpRatings(exposure, obserscanBbox, dataBbox)
        ipIsr.overscanCorrection(exposure, overscanBbox, fittype)
        #TODO optionally trim
        #output products
        clipboard.put(self.policy.get("outputKeys.overscanCorrectedExposure"), exposure)
        
class IsrOverscanStage(harnessStage.Stage):
    parallelClass = IsrOverscanStageParallel

