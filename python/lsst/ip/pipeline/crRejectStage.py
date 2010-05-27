#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.utils as ipUtils
import lsst.afw.display.ds9 as ds9

try:
    type(display)
except NameError:
    display = False

class CrRejectStageParallel(harnessStage.ParallelProcessing):
    """
    Description:
        This stage wraps estimating and possibly subtracting cosmic rays from an exposure
        on the clipboard.        

    Policy Dictionary:
    lsst/ip/pipeline/policy/CrRejectStageDictionary.paf

    Clipboard Input:
    - Calibrated science Exposure(s) (without background)
    - a PSF may be specified by policy attribute inputPsfKey. Alternatively, the
      stage's policy may request that a psf be constructed, by providing the
      psfPolicy attribute.

    ClipboardOutput:
    - Exposure with CRs removed. Key specified
        by policy attribute 'crSubtractedExposureKey'
    - nCR The number of CRs detected
    - PSF: the psf used to smooth the exposure before detection 
        Key specified by policy attribute 'psfKey'
    """
    def setup(self):
        self.log = Log(self.log, "CrRejectStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "CrRejectStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath(), True)

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy.getDictionary())

        self.crRejectPolicy = self.policy.get("crRejectPolicy")

    def process(self, clipboard):
        """
        Detect CRs in the worker process
        """
        self.log.log(Log.INFO, "Detecting CRs in process")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.get("inputKeys.exposure"))

        defaultFwhm = self.policy.get('parameters.defaultFwhm') # in arcsec
        keepCRs = self.policy.get('parameters.keepCRs')

        self.crRejectPolicy.set('min_sigma', self.crRejectPolicy.get('minSigma'))

        crs = ipUtils.cosmicRays.findCosmicRays(exposure, self.crRejectPolicy, defaultFwhm, keepCRs)
        ds9.mtv(exposure, frame=0)
        nCR = len(crs)

        #output products
        clipboard.put("nCR", nCR)
        clipboard.put(self.policy.get("outputKeys.exposure"), exposure)
        
class CrRejectStage(harnessStage.Stage):
    parallelClass = CrRejectStageParallel

