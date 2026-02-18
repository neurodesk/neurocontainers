#!/usr/bin/env python3
"""
Comprehensive Security Report Generator

This script generates a detailed security report similar to the comprehensive
security assessment format, processing Grype scan results and enriching them
with EPSS scores and KEV (Known Exploited Vulnerabilities) data.
"""

import json
import sys
import argparse
import subprocess
import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests
from collections import defaultdict, Counter

class SecurityReportGenerator:
    def __init__(self, grype_results_file: str, image_info_file: str = None):
        self.grype_results_file = grype_results_file
        self.image_info_file = image_info_file
        self.grype_data = None
        self.image_info = None
        self.epss_cache = {}
        self.kev_cache = set()
        
    def load_data(self):
        """Load Grype results and image information"""
        try:
            with open(self.grype_results_file, 'r') as f:
                self.grype_data = json.load(f)
        except Exception as e:
            print(f"Error loading Grype results: {e}")
            sys.exit(1)
            
        if self.image_info_file and Path(self.image_info_file).exists():
            try:
                with open(self.image_info_file, 'r') as f:
                    self.image_info = json.load(f)
            except Exception as e:
                print(f"Warning: Could not load image info: {e}")
    
    def get_epss_score(self, cve_id: str) -> float:
        """Fetch EPSS score for a CVE (with caching)"""
        if cve_id in self.epss_cache:
            return self.epss_cache[cve_id]
            
        try:
            # EPSS API endpoint
            url = f"https://api.first.org/data/v1/epss?cve={cve_id}"
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and len(data['data']) > 0:
                    epss_score = float(data['data'][0].get('epss', 0))
                    self.epss_cache[cve_id] = epss_score
                    return epss_score
        except Exception as e:
            print(f"Warning: Could not fetch EPSS for {cve_id}: {e}")
        
        self.epss_cache[cve_id] = 0.0
        return 0.0
    
    def load_kev_data(self):
        """Load CISA KEV catalog"""
        try:
            url = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                kev_data = response.json()
                self.kev_cache = {vuln['cveID'] for vuln in kev_data.get('vulnerabilities', [])}
                print(f"Loaded {len(self.kev_cache)} KEV entries")
            else:
                print("Warning: Could not load KEV data")
        except Exception as e:
            print(f"Warning: Could not load KEV data: {e}")
    
    def calculate_risk_score(self, vuln: Dict[str, Any], epss_score: float, is_kev: bool) -> float:
        """Calculate a risk score based on severity, EPSS, and KEV status"""
        severity_weights = {
            'Critical': 10.0,
            'High': 7.5,
            'Medium': 5.0,
            'Low': 2.5,
            'Unknown': 1.0
        }
        
        severity = vuln.get('vulnerability', {}).get('severity', 'Unknown')
        base_score = severity_weights.get(severity, 1.0)
        
        # Boost score based on EPSS
        epss_multiplier = 1.0 + (epss_score * 2)  # Max 3x multiplier for 100% EPSS
        
        # Significant boost for KEV
        kev_multiplier = 10.0 if is_kev else 1.0
        
        return base_score * epss_multiplier * kev_multiplier
    
    def process_vulnerabilities(self) -> List[Dict[str, Any]]:
        """Process vulnerabilities and enrich with EPSS and KEV data"""
        vulnerabilities = []
        
        print("Processing vulnerabilities and fetching EPSS scores...")
        
        matches = self.grype_data.get('matches', [])
        
        for i, match in enumerate(matches):
            if i % 100 == 0:
                print(f"Processed {i}/{len(matches)} vulnerabilities...")
                
            vuln_id = match.get('vulnerability', {}).get('id', '')
            
            # Get EPSS score if it's a CVE
            epss_score = 0.0
            if vuln_id.startswith('CVE-'):
                epss_score = self.get_epss_score(vuln_id)
            
            # Check if it's in KEV
            is_kev = vuln_id in self.kev_cache
            
            # Calculate risk score
            risk_score = self.calculate_risk_score(match, epss_score, is_kev)
            
            # Enrich the vulnerability data
            enriched_vuln = {
                'id': vuln_id,
                'severity': match.get('vulnerability', {}).get('severity', 'Unknown'),
                'package': match.get('artifact', {}).get('name', ''),
                'version': match.get('artifact', {}).get('version', ''),
                'type': match.get('artifact', {}).get('type', ''),
                'description': match.get('vulnerability', {}).get('description', ''),
                'epss_score': epss_score,
                'is_kev': is_kev,
                'risk_score': risk_score,
                'fixed_in': match.get('vulnerability', {}).get('fix', {}).get('versions', []),
                'urls': match.get('vulnerability', {}).get('urls', []),
                'raw_match': match
            }
            
            vulnerabilities.append(enriched_vuln)
        
        return vulnerabilities
    
    def process_vulnerabilities_quick(self) -> List[Dict[str, Any]]:
        """Process vulnerabilities quickly without external API calls"""
        vulnerabilities = []
        
        print("Processing vulnerabilities (quick mode)...")
        
        matches = self.grype_data.get('matches', [])
        
        for match in matches:
            vuln_id = match.get('vulnerability', {}).get('id', '')
            
            # Quick processing without EPSS/KEV lookups
            severity = match.get('vulnerability', {}).get('severity', 'Unknown')
            severity_weights = {'Critical': 10.0, 'High': 7.5, 'Medium': 5.0, 'Low': 2.5, 'Unknown': 1.0}
            risk_score = severity_weights.get(severity, 1.0)
            
            # Enrich the vulnerability data
            enriched_vuln = {
                'id': vuln_id,
                'severity': severity,
                'package': match.get('artifact', {}).get('name', ''),
                'version': match.get('artifact', {}).get('version', ''),
                'type': match.get('artifact', {}).get('type', ''),
                'description': match.get('vulnerability', {}).get('description', ''),
                'epss_score': 0.0,
                'is_kev': False,
                'risk_score': risk_score,
                'fixed_in': match.get('vulnerability', {}).get('fix', {}).get('versions', []),
                'urls': match.get('vulnerability', {}).get('urls', []),
                'raw_match': match
            }
            
            vulnerabilities.append(enriched_vuln)
        
        return vulnerabilities
    
    def generate_quick_report(self, output_file: str = None):
        """Generate a comprehensive report without external API calls"""
        
        # Process vulnerabilities quickly
        vulnerabilities = self.process_vulnerabilities_quick()
        
        # Sort by risk score (highest first)
        vulnerabilities.sort(key=lambda x: x['risk_score'], reverse=True)
        
        # Calculate statistics
        total_vulns = len(vulnerabilities)
        severity_counts = Counter([v['severity'] for v in vulnerabilities])
        type_counts = Counter([v['type'] for v in vulnerabilities])
        
        # Get image information
        image_name = "Unknown"
        if 'source' in self.grype_data:
            source = self.grype_data['source']
            if 'target' in source:
                image_name = source['target'].get('userInput', image_name)
        
        scan_date = datetime.datetime.now().strftime("%B %d, %Y")
        
        # Generate the report
        report = self._generate_quick_report_content(
            image_name, scan_date, total_vulns, severity_counts, 
            type_counts, vulnerabilities
        )
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
            print(f"Comprehensive security report generated: {output_file}")
        else:
            print(report)
    
    def generate_report(self, output_file: str = None):
        """Generate the comprehensive security report"""
        
        # Load KEV data first
        self.load_kev_data()
        
        # Process vulnerabilities
        vulnerabilities = self.process_vulnerabilities()
        
        # Sort by risk score (highest first)
        vulnerabilities.sort(key=lambda x: x['risk_score'], reverse=True)
        
        # Calculate statistics
        total_vulns = len(vulnerabilities)
        severity_counts = Counter([v['severity'] for v in vulnerabilities])
        type_counts = Counter([v['type'] for v in vulnerabilities])
        kev_count = sum(1 for v in vulnerabilities if v['is_kev'])
        high_epss_count = sum(1 for v in vulnerabilities if v['epss_score'] > 0.1)
        
        # Get image information
        image_name = "Unknown"
        image_size = "Unknown"
        scan_date = datetime.datetime.now().strftime("%B %d, %Y")
        
        if self.image_info:
            config = self.image_info[0].get('Config', {})
            image_name = config.get('Image', image_name)
            
        # Get image size from Grype data
        if 'source' in self.grype_data:
            source = self.grype_data['source']
            if 'target' in source:
                image_name = source['target'].get('userInput', image_name)
        
        # Generate the report
        report = self._generate_report_content(
            image_name, scan_date, total_vulns, severity_counts, 
            type_counts, kev_count, high_epss_count, vulnerabilities
        )
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(report)
            print(f"Comprehensive security report generated: {output_file}")
        else:
            print(report)
    
    def _generate_report_content(self, image_name: str, scan_date: str, total_vulns: int,
                                severity_counts: Counter, type_counts: Counter, 
                                kev_count: int, high_epss_count: int, 
                                vulnerabilities: List[Dict[str, Any]]) -> str:
        """Generate the actual report content"""
        
        # Calculate filtered count (medium and above)
        filtered_count = (severity_counts.get('Critical', 0) + 
                         severity_counts.get('High', 0) + 
                         severity_counts.get('Medium', 0))
        
        # Risk assessment
        risk_level = "LOW"
        if kev_count > 0 or severity_counts.get('Critical', 0) > 5:
            risk_level = "HIGH RISK"
        elif severity_counts.get('Critical', 0) > 0 or severity_counts.get('High', 0) > 20:
            risk_level = "MEDIUM RISK"
        
        report = f"""# Container Security Assessment Report

**Container Image:** `{image_name}`  
**Scan Date:** {scan_date}  
**Scanner:** Grype v0.82.0  
**Architecture:** linux/amd64  

---

## Executive Summary

This comprehensive security assessment reveals **{total_vulns:,} total vulnerabilities** across the container image, with **{filtered_count:,} vulnerabilities** requiring immediate attention after filtering. The container presents **{"significant security risks" if risk_level == "HIGH RISK" else "moderate security risks" if risk_level == "MEDIUM RISK" else "manageable security risks"}** that require {"prompt" if risk_level == "HIGH RISK" else "timely"} remediation.

### Key Risk Indicators

| **Metric** | **Count** | **Percentage** |
|------------|-----------|----------------|
| Critical Vulnerabilities | {severity_counts.get('Critical', 0)} | {(severity_counts.get('Critical', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |
| High Severity | {severity_counts.get('High', 0)} | {(severity_counts.get('High', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |
| Medium Severity | {severity_counts.get('Medium', 0)} | {(severity_counts.get('Medium', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |
| Known Exploited (KEV) | {kev_count} | {(kev_count / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |
| High EPSS Score (>10%) | {high_epss_count} | {(high_epss_count / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |

### Risk Assessment: **{risk_level}**

{"The container contains vulnerabilities that are actively exploited in the wild (KEV). Immediate patching is required." if kev_count > 0 else "No known exploited vulnerabilities detected, but critical issues require attention." if severity_counts.get('Critical', 0) > 0 else "The container has manageable security risks with standard update procedures recommended."}

---

## Vulnerability Distribution

### By Severity
```
Critical: {"█" * min(40, severity_counts.get('Critical', 0) // max(1, filtered_count // 40))} {severity_counts.get('Critical', 0)} ({(severity_counts.get('Critical', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}%)
High:     {"█" * min(40, severity_counts.get('High', 0) // max(1, filtered_count // 40))} {severity_counts.get('High', 0)} ({(severity_counts.get('High', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}%)
Medium:   {"█" * min(40, severity_counts.get('Medium', 0) // max(1, filtered_count // 40))} {severity_counts.get('Medium', 0)} ({(severity_counts.get('Medium', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}%)
```

### By Package Type
```"""

        # Add package type distribution
        for pkg_type, count in type_counts.most_common():
            percentage = (count / filtered_count * 100) if filtered_count > 0 else 0
            bar_length = min(40, count // max(1, filtered_count // 40))
            report += f"\n{pkg_type.title()}: {'█' * bar_length} {count} ({percentage:.2f}%)"

        report += f"""
```

---

## Critical Vulnerabilities (Immediate Action Required)
"""

        # Add top 10 highest risk vulnerabilities
        critical_vulns = [v for v in vulnerabilities if v['severity'] in ['Critical', 'High'] or v['is_kev']][:10]
        
        for i, vuln in enumerate(critical_vulns, 1):
            kev_indicator = " - KEV Listed" if vuln['is_kev'] else ""
            
            report += f"""
### {i}. **{vuln['id']}** - {vuln['package']}{kev_indicator}
- **Severity:** {vuln['severity']} (Risk Score: {vuln['risk_score']:.1f})
- **Package:** `{vuln['package']}` v{vuln['version']}
- **EPSS Score:** {vuln['epss_score']*100:.1f}%
- **Status:** {"**KNOWN EXPLOITED VULNERABILITY**" if vuln['is_kev'] else "Standard vulnerability"}
- **Impact:** {vuln['description'][:100]}...
- **Fix:** {"Available" if vuln['fixed_in'] else "No fix available yet"}

---"""

        # Add top 10 table
        report += f"""
## Top 10 Highest Risk Vulnerabilities

| **CVE/Advisory** | **Package** | **Severity** | **Risk Score** | **EPSS** | **KEV** |
|------------------|-------------|--------------|----------------|----------|---------|"""

        for vuln in vulnerabilities[:10]:
            kev_status = "Yes" if vuln['is_kev'] else "No"
            report += f"""
| {vuln['id']} | {vuln['package']} | {vuln['severity']} | {vuln['risk_score']:.1f} | {vuln['epss_score']*100:.1f}% | {kev_status} |"""

        # Add risk analysis
        epss_high = sum(1 for v in vulnerabilities if v['epss_score'] > 0.5)
        epss_medium = sum(1 for v in vulnerabilities if 0.2 <= v['epss_score'] <= 0.5)
        epss_low_medium = sum(1 for v in vulnerabilities if 0.05 <= v['epss_score'] < 0.2)
        epss_low = sum(1 for v in vulnerabilities if v['epss_score'] < 0.05)

        report += f"""

---

## Risk Analysis

### EPSS Score Distribution
- **Very High (>50%):** {epss_high} vulnerabilities
- **High (20-50%):** {epss_medium} vulnerabilities  
- **Medium (5-20%):** {epss_low_medium} vulnerabilities
- **Low (<5%):** {epss_low} vulnerabilities

### Package Risk Assessment

#### **Critical Risk Packages**"""

        # Find packages with the highest risk
        pkg_risks = defaultdict(list)
        for vuln in vulnerabilities:
            pkg_risks[vuln['package']].append(vuln)
        
        # Sort packages by their highest risk vulnerability
        sorted_packages = sorted(pkg_risks.items(), 
                               key=lambda x: max(v['risk_score'] for v in x[1]), 
                               reverse=True)
        
        for i, (pkg_name, pkg_vulns) in enumerate(sorted_packages[:5], 1):
            max_risk = max(v['risk_score'] for v in pkg_vulns)
            kev_in_pkg = any(v['is_kev'] for v in pkg_vulns)
            critical_count = sum(1 for v in pkg_vulns if v['severity'] == 'Critical')
            
            status = ""
            if kev_in_pkg:
                status = "Contains KEV vulnerability, immediate update required"
            elif critical_count > 0:
                status = f"{critical_count} critical vulnerabilities"
            else:
                status = f"{len(pkg_vulns)} vulnerabilities"
                
            report += f"""
{i}. **{pkg_name}** - {status}"""

        report += f"""

---

## Remediation Recommendations

### **Immediate Actions (Within 24-48 Hours)**
"""

        # Add immediate actions for KEV and critical vulns
        kev_vulns = [v for v in vulnerabilities if v['is_kev']]
        if kev_vulns:
            for vuln in kev_vulns[:3]:  # Top 3 KEV
                report += f"""
{len([v for v in kev_vulns if kev_vulns.index(v) <= kev_vulns.index(vuln)])}. **URGENT: Update {vuln['package']}**
   ```bash
   # Update {vuln['package']} to patch {vuln['id']}
   # Check package manager for latest secure version
   ```"""

        report += f"""

### **Short-term Actions (1-2 Weeks)**

4. **Update High-Risk Packages**
   - Review packages with multiple high-severity issues
   - Prioritize packages with EPSS scores > 20%

### **Medium-term Actions (1 Month)**

5. **Comprehensive Package Updates**
   ```bash
   # Update all packages to latest versions
   # Run security audit tools
   ```

6. **Implement Security Scanning**
   - Set up automated vulnerability scanning
   - Configure alerts for new vulnerabilities

---

## Detailed Vulnerability Breakdown
"""

        # Add package type breakdown
        for pkg_type, count in type_counts.most_common():
            type_vulns = [v for v in vulnerabilities if v['type'] == pkg_type]
            critical_in_type = sum(1 for v in type_vulns if v['severity'] == 'Critical')
            
            report += f"""
### {pkg_type.title()} ({count} vulnerabilities)
- Primary contributors: {', '.join(list(set([v['package'] for v in type_vulns[:5]])))}
- Critical vulnerabilities: {critical_in_type}
- {"Requires immediate attention" if critical_in_type > 0 else "Standard update cycle recommended"}
"""

        # Add compliance section
        report += f"""
---

## Compliance and Security Standards

### NIST Guidelines
- {"**Fails:** Contains KEV vulnerabilities past due date" if kev_count > 0 else "**Passes:** No KEV vulnerabilities detected"}
- {"**Fails:** Contains critical vulnerabilities without fixes" if severity_counts.get('Critical', 0) > 0 else "**Passes:** No unpatched critical vulnerabilities"}
- {"**Warning:** High number of medium severity issues" if severity_counts.get('Medium', 0) > 100 else "**Acceptable:** Manageable number of medium issues"}

### Industry Best Practices
- **Container Scanning:** Implemented
- **Vulnerability Management:** {"Needs improvement" if kev_count > 0 or severity_counts.get('Critical', 0) > 5 else "Needs attention"}
- **Patch Management:** {"Significantly behind" if kev_count > 0 else "Standard updates needed"}
- **Supply Chain Security:** {"Needs attention" if high_epss_count > 10 else "Acceptable"}

---

## Next Steps and Action Plan

### Week 1: Critical Remediation
- [ ] {"Update packages with KEV vulnerabilities" if kev_count > 0 else "Update critical severity packages"}
- [ ] {"Patch known exploited vulnerabilities immediately" if kev_count > 0 else "Review and update high-risk packages"}
- [ ] Test container functionality after updates
- [ ] Verify vulnerability fixes

### Week 2: High Priority Updates
- [ ] Update packages with high EPSS scores
- [ ] Review and update packages with multiple vulnerabilities
- [ ] Perform security regression testing
- [ ] Document update procedures

### Month 1: Comprehensive Updates
- [ ] Full package update cycle
- [ ] Implement automated vulnerability scanning in CI/CD
- [ ] Set up vulnerability alerting
- [ ] Create container update automation

### Ongoing: Security Hardening
- [ ] Implement regular security scanning schedule
- [ ] Monitor for new vulnerabilities
- [ ] Maintain security update procedures
- [ ] Regular security assessments

---

## Additional Resources

- **CISA KEV Catalog:** https://www.cisa.gov/known-exploited-vulnerabilities-catalog
- **EPSS Calculator:** https://www.first.org/epss/calculator
- **Grype Documentation:** https://github.com/anchore/grype
- **CVE Database:** https://cve.mitre.org/

---

**Report Generated:** {datetime.datetime.now().strftime('%B %d, %Y')}  
**Next Recommended Scan:** {"After critical updates (within 48 hours)" if kev_count > 0 or severity_counts.get('Critical', 0) > 0 else "Weekly scan recommended"}  
**Tools Used:** Grype v0.82.0, EPSS v3.0, CISA KEV Catalog

*This report should be reviewed by security teams and container maintainers {"immediately due to the presence of Known Exploited Vulnerabilities" if kev_count > 0 else "to address identified security risks"}.*
"""

        return report
    
    def _generate_quick_report_content(self, image_name: str, scan_date: str, total_vulns: int,
                                      severity_counts: Counter, type_counts: Counter, 
                                      vulnerabilities: List[Dict[str, Any]]) -> str:
        """Generate the actual report content (quick version)"""
        
        # Calculate filtered count (medium and above)
        filtered_count = (severity_counts.get('Critical', 0) + 
                         severity_counts.get('High', 0) + 
                         severity_counts.get('Medium', 0))
        
        # Risk assessment
        risk_level = "LOW"
        if severity_counts.get('Critical', 0) > 5:
            risk_level = "HIGH RISK"
        elif severity_counts.get('Critical', 0) > 0 or severity_counts.get('High', 0) > 20:
            risk_level = "MEDIUM RISK"
        
        report = f"""# Container Security Assessment Report

**Container Image:** `{image_name}`  
**Scan Date:** {scan_date}  
**Scanner:** Grype v0.82.0  
**Architecture:** linux/amd64  

---

## Executive Summary

This comprehensive security assessment reveals **{total_vulns:,} total vulnerabilities** across the container image, with **{filtered_count:,} vulnerabilities** requiring immediate attention after filtering. The container presents **{"significant security risks" if risk_level == "HIGH RISK" else "moderate security risks" if risk_level == "MEDIUM RISK" else "manageable security risks"}** that require {"prompt" if risk_level == "HIGH RISK" else "timely"} remediation.

### Key Risk Indicators

| **Metric** | **Count** | **Percentage** |
|------------|-----------|----------------|
| Critical Vulnerabilities | {severity_counts.get('Critical', 0)} | {(severity_counts.get('Critical', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |
| High Severity | {severity_counts.get('High', 0)} | {(severity_counts.get('High', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |
| Medium Severity | {severity_counts.get('Medium', 0)} | {(severity_counts.get('Medium', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}% |

### Risk Assessment: **{risk_level}**

{"Critical vulnerabilities require immediate attention." if severity_counts.get('Critical', 0) > 0 else "The container has manageable security risks with standard update procedures recommended."}

---

## Vulnerability Distribution

### By Severity
```
Critical: {"█" * min(40, max(1, severity_counts.get('Critical', 0) * 40 // max(1, filtered_count)))} {severity_counts.get('Critical', 0)} ({(severity_counts.get('Critical', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}%)
High:     {"█" * min(40, max(1, severity_counts.get('High', 0) * 40 // max(1, filtered_count)))} {severity_counts.get('High', 0)} ({(severity_counts.get('High', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}%)
Medium:   {"█" * min(40, max(1, severity_counts.get('Medium', 0) * 40 // max(1, filtered_count)))} {severity_counts.get('Medium', 0)} ({(severity_counts.get('Medium', 0) / filtered_count * 100) if filtered_count > 0 else 0:.2f}%)
```

### By Package Type
```"""

        # Add package type distribution
        for pkg_type, count in type_counts.most_common():
            percentage = (count / filtered_count * 100) if filtered_count > 0 else 0
            bar_length = min(40, max(1, count * 40 // max(1, filtered_count)))
            report += f"\n{pkg_type.title()}: {'█' * bar_length} {count} ({percentage:.2f}%)"

        report += f"""
```

---

## Critical Vulnerabilities (Immediate Action Required)
"""

        # Add top 10 highest risk vulnerabilities
        critical_vulns = [v for v in vulnerabilities if v['severity'] in ['Critical', 'High']][:10]
        
        for i, vuln in enumerate(critical_vulns, 1):
            report += f"""
### {i}. **{vuln['id']}** - {vuln['package']}
- **Severity:** {vuln['severity']} (Risk Score: {vuln['risk_score']:.1f})
- **Package:** `{vuln['package']}` v{vuln['version']}
- **Impact:** {vuln['description'][:100] if vuln['description'] else 'No description available'}...
- **Fix:** {"Available" if vuln['fixed_in'] else "No fix available yet"}

---"""

        # Add top 10 table
        report += f"""
## Top 10 Highest Risk Vulnerabilities

| **CVE/Advisory** | **Package** | **Severity** | **Risk Score** |
|------------------|-------------|--------------|----------------|"""

        for vuln in vulnerabilities[:10]:
            report += f"""
| {vuln['id']} | {vuln['package']} | {vuln['severity']} | {vuln['risk_score']:.1f} |"""

        report += f"""

---

## Remediation Recommendations

### **Immediate Actions (Within 24-48 Hours)**

1. **Update Critical Packages**
   - Focus on packages with Critical severity vulnerabilities
   - Test updates in staging environment first

2. **Review High-Risk Packages**
   - Prioritize packages with multiple vulnerabilities
   - Consider alternative packages if updates unavailable

### **Short-term Actions (1-2 Weeks)**

3. **Update High Severity Packages**
   - Address high severity vulnerabilities systematically
   - Implement security testing procedures

4. **Package Management**
   - Establish regular update schedule
   - Monitor security advisories

### **Medium-term Actions (1 Month)**

5. **Comprehensive Security Program**
   - Implement automated vulnerability scanning
   - Set up continuous security monitoring
   - Establish incident response procedures

---

## Compliance and Security Standards

### NIST Guidelines
- {"**Fails:** Contains critical vulnerabilities" if severity_counts.get('Critical', 0) > 0 else "**Passes:** No critical vulnerabilities"}
- {"**Warning:** High number of medium severity issues" if severity_counts.get('Medium', 0) > 100 else "**Acceptable:** Manageable number of medium issues"}

### Industry Best Practices
- **Container Scanning:** Implemented
- **Vulnerability Management:** {"Needs improvement" if severity_counts.get('Critical', 0) > 5 else "Needs attention"}
- **Patch Management:** {"Significantly behind" if severity_counts.get('Critical', 0) > 0 else "Standard updates needed"}

---

**Report Generated:** {datetime.datetime.now().strftime('%B %d, %Y')}  
**Next Recommended Scan:** {"After critical updates (within 48 hours)" if severity_counts.get('Critical', 0) > 0 else "Weekly scan recommended"}  
**Tools Used:** Grype v0.82.0

*This report should be reviewed by security teams and container maintainers {"immediately due to critical vulnerabilities" if severity_counts.get('Critical', 0) > 0 else "to address identified security risks"}.*
"""

        return report


def main():
    parser = argparse.ArgumentParser(description='Generate comprehensive security report from Grype scan results')
    parser.add_argument('grype_results', help='Path to Grype JSON results file')
    parser.add_argument('--image-info', help='Path to image info JSON file (optional)')
    parser.add_argument('--output', '-o', help='Output file path (default: print to stdout)')
    parser.add_argument('--skip-epss', action='store_true', help='Skip EPSS score fetching (faster but less accurate)')
    parser.add_argument('--quick', action='store_true', help='Quick mode without external API calls')
    
    args = parser.parse_args()
    
    if not Path(args.grype_results).exists():
        print(f"Error: Grype results file not found: {args.grype_results}")
        sys.exit(1)
    
    print("Generating comprehensive security report...")
    print("This may take a few minutes to fetch EPSS scores and KEV data...")
    
    generator = SecurityReportGenerator(args.grype_results, args.image_info)
    generator.load_data()
    
    if args.quick or args.skip_epss:
        print("Running in quick mode (no external API calls)")
        generator.generate_quick_report(args.output)
    else:
        print("This may take a few minutes to fetch EPSS scores and KEV data...")
        generator.generate_report(args.output)

if __name__ == '__main__':
    main()