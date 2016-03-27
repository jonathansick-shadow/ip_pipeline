#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

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

        # grab exposure from clipboard
        exposure = clipboard.get(self.policy.get("inputKeys.exposure"))

        defaultFwhm = self.policy.get('parameters.defaultFwhm')  # in arcsec
        keepCRs = self.policy.get('parameters.keepCRs')

        crs = ipUtils.cosmicRays.findCosmicRays(exposure, self.crRejectPolicy, defaultFwhm, keepCRs)
        nCR = len(crs)

        # output products
        clipboard.put("nCR", nCR)
        clipboard.put(self.policy.get("outputKeys.exposure"), exposure)


class CrRejectStage(harnessStage.Stage):
    parallelClass = CrRejectStageParallel

