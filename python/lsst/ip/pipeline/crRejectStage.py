#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.afw.detection as afwDet
import lsst.afw.image as afwImg
import lsst.afw.math as afwMath
import lsst.pex.exceptions as pexExcept
import lsst.meas.algorithms as measAlg

import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as displayUtils

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
        self.policy.mergeDefaults(defPolicy)

        self.crRejectPolicy = self.policy.get("crRejectPolicy")

    def process(self, clipboard):
        """
        Detect CRs in the worker process
        """
        self.log.log(Log.INFO, "Detecting CRs in process")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.get("inputKeys.exposure"))

        self.crRejectPolicy.set('gain', exposure.getMetadata().get('GAIN'))
        # Set backwards compatible names; should fix meas/algorithms
        self.crRejectPolicy.set('e_per_dn', self.crRejectPolicy.get('gain'))
        self.crRejectPolicy.set('min_sigma', self.crRejectPolicy.get('minSigma'))

        mi = exposure.getMaskedImage()
        wcs = exposure.getWcs()

        defaultFwhm = self.policy.get('parameters.defaultFwhm') # in arcsec
        scale = math.sqrt(wcs.pixArea(afwImg.PointD(mi.getWidth()/2, mi.getHeight()/2)))*3600 # arcsec/pixel
        defaultFwhm /= scale            # convert to pixels
        
        psf         = measAlg.createPSF('DoubleGaussian', 0, 0, defaultFwhm/(2*math.sqrt(2*math.log(2))))

        bg = afwMath.makeStatistics(mi, afwMath.MEDIAN).getValue()
        crs = measAlg.findCosmicRays(mi, psf, bg, self.crRejectPolicy,
                                     self.policy.get('parameters.keepCRs'))
        nCR = len(crs)

        #output products
        clipboard.put("nCR", nCR)
        clipboard.put(self.policy.get("outputKeys.exposure"), exposure)
        
class CrRejectStage(harnessStage.Stage):
    parallelClass = CrRejectStageParallel

