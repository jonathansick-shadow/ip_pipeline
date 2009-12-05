#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.image as afwImage

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

try:
    type(display)
except NameError:
    display = False

class SimpleDiffImStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        Subtract two almost-identical Exposures

    Policy Dictionary:
        lsst/ip/pipeline/policy/simpleDiffImStageDictionary.paf

    Clipboard Input:
    - Two calibrated science Exposures

    ClipboardOutput:
    - Difference Exposure
    """
    def setup(self):
        self.log = Log(self.log, "simpleDiffImStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "simpleDiffImStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        Subtract two almost-identical Exposures
        """
        self.log.log(Log.INFO, "Differencing two Exposures in process")
        
        #grab exposure from clipboard
        exposures = []
        for k in self.policy.getArray("inputKeys.exposures"):
            exposures.append(clipboard.get(k))

        mi0 = exposures[0].getMaskedImage()
        diff = mi0.Factory(mi0, True)
        diff -= exposures[1].getMaskedImage()

        differenceExposure = afwImage.makeExposure(diff, exposures[0].getWcs())
        differenceExposure.setMetadata(exposures[0].getMetadata())

        #output products
        clipboard.put(self.policy.get("outputKeys.differenceExposure"), differenceExposure)
        
class SimpleDiffImStage(harnessStage.Stage):
    parallelClass = SimpleDiffImStageParallel

