"""
Medication Deduplicator - Handles medication deduplication logic
"""
from typing import List, Dict
import logging
import re
import copy
# from healthdata_connect.config.logging_config import get_log_level

logger = logging.getLogger(__name__)
# logger.setLevel(get_log_level())


class MedicationDeduplicator:
    """Handles deduplication and merging of medication records."""
    
    def deduplicate_medications(self, medications: List[Dict]) -> List[Dict]:
        """
        Deduplicate medications by grouping similar medication names.
        
        :param medications: List of medication dictionaries
        :return: Deduplicated list of medications
        """
        if not medications:
            return []
        
        logging.debug(f"Starting deduplication with {len(medications)} medications")
        
        # Group medications by normalized name
        medication_groups = {}
        
        for medication in medications:
            # Support both new format (name) and legacy format (medication_name)
            name = medication.get('name', '') or medication.get('medication_name', '')
            normalized_name = self.normalize_medication_name(name)
            
            if normalized_name not in medication_groups:
                medication_groups[normalized_name] = []
            medication_groups[normalized_name].append(medication)
        
        logging.debug(f"Grouped medications into {len(medication_groups)} groups")
        
        # Log duplicate groups only
        for norm_name, group in medication_groups.items():
            if len(group) > 1:
                # Don't log actual medication names - PII/PHI
                logging.debug(f"Found duplicate group with {len(group)} medications")
        
        # Merge duplicates within each group
        deduplicated = []
        for normalized_name, group in medication_groups.items():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                # Multiple medications with same normalized name - merge them
                logging.debug(f"Merging {len(group)} duplicate medications")
                merged = self.merge_duplicate_medications(group)
                deduplicated.append(merged)
                # Don't log actual medication name - PII/PHI
                logging.debug(f"Merged medication with {len(merged.get('prescription_history', []))} historical entries")
        
        logging.debug(f"Deduplicated to {len(deduplicated)} medications")
        return deduplicated

    def normalize_medication_name(self, name: str) -> str:
        """
        Normalize medication name for grouping duplicates.
        
        :param name: Raw medication name
        :return: Normalized medication name for comparison
        """
        if not name:
            return ""
        
        # Convert to lowercase and strip
        normalized = name.lower().strip()
        
        # Handle specific medication name variations
        # Omega-3 / Fish Oil normalizations
        normalized = re.sub(r'\bomega-?3\b', 'omega3', normalized)
        normalized = re.sub(r'\bfish\s+oil?\b', 'fishoil', normalized)
        
        # Handle common spelling variations
        normalized = re.sub(r'\bvitamin\s+d\d?\b', 'vitamind', normalized)
        normalized = re.sub(r'\bvitamin\s+b\d+\b', 'vitaminb', normalized)
        normalized = re.sub(r'\bvitamin\s+c\b', 'vitaminc', normalized)
        
        # Remove common dosage information that might be embedded in names
        normalized = re.sub(r'\b\d+(?:[,.-]\d+)*\s*(?:mg|g|ml|mcg|units?|iu)\b', '', normalized)
        
        # Remove common form indicators
        normalized = re.sub(r'\b(?:tablet|tablets|capsule|capsules|cap|caps|pill|pills|injection|oral|topical|softgel|softgels)\b', '', normalized)
        
        # Remove brand vs generic indicators
        normalized = re.sub(r'\b(?:generic|brand|name brand)\b', '', normalized)
        
        # Remove dosage ranges and complex dosage info
        normalized = re.sub(r'\b\d+(?:[,.-]\d+)*\s*-\s*\d+(?:[,.-]\d+)*\s*(?:mg|g|ml|mcg|units?|iu)\b', '', normalized)
        
        # Remove extra whitespace and punctuation
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # Remove common words that don't affect medication identity
        normalized = re.sub(r'\b(?:the|a|an|for|with|extended|release|immediate|delayed|extra|strength|maximum|regular)\b', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        # Remove standalone numbers that remain
        normalized = re.sub(r'\b\d+\b', '', normalized).strip()
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized

    def merge_duplicate_medications(self, medications: list) -> Dict:
        """
        Merge a list of duplicate medications into a single medication with combined information.
        Prioritizes most recent prescription information for current instructions.
        
        :param medications: List of medication dictionaries with the same name
        :return: Single merged medication dictionary
        """
        if not medications:
            return {}
        
        if len(medications) == 1:
            return medications[0]
        
        # Sort medications by prescribed date (most recent first)
        medications_with_dates = []
        medications_without_dates = []
        
        for med in medications:
            # Support both new format (date) and legacy format (prescribed_on)
            prescribed_on = med.get("date") or med.get("prescribed_on")
            
            # Only process if it's a string date in a valid format
            if isinstance(prescribed_on, str) and prescribed_on:
                try:
                    # Handle different date formats for sorting
                    if "-" in prescribed_on and len(prescribed_on) == 10:  # YYYY-MM-DD
                        medications_with_dates.append((prescribed_on, med))
                    elif "/" in prescribed_on:  # MM/DD/YYYY
                        medications_with_dates.append((prescribed_on, med))
                    else:
                        medications_without_dates.append(med)
                except Exception:
                    medications_without_dates.append(med)
            else:
                # Not a string or empty - can't use for date sorting
                medications_without_dates.append(med)
        
        # Sort by date (most recent first)
        medications_with_dates.sort(key=lambda x: x[0], reverse=True)
        
        # Get the most recent medication as the base
        if medications_with_dates:
            most_recent = medications_with_dates[0][1]
        else:
            most_recent = medications[0]
        
        # Start with the most recent medication as base
        # Deep copy to avoid modifying original
        merged = copy.deepcopy(most_recent)
        
        # Collect all prescribed dates for history
        prescribed_dates = []
        all_dosage_amounts = set()
        all_statuses = set()
        
        # Process medications with dates first (in chronological order)
        for date, med in medications_with_dates:
            prescribed_dates.append(date)
            
            # Handle both new format (dosage object) and legacy format (dosage string)
            dosage = med.get("dosage", {})
            if isinstance(dosage, dict):
                amount = dosage.get("amount", "")
                if amount:
                    all_dosage_amounts.add(amount)
            elif isinstance(dosage, str) and dosage:
                all_dosage_amounts.add(dosage)
            
            status = med.get("status", "")
            if isinstance(status, str) and status:
                all_statuses.add(status)
        
        # Process medications without dates
        for med in medications_without_dates:
            # Handle both new format (dosage object) and legacy format (dosage string)
            dosage = med.get("dosage", {})
            if isinstance(dosage, dict):
                amount = dosage.get("amount", "")
                if amount:
                    all_dosage_amounts.add(amount)
            elif isinstance(dosage, str) and dosage:
                all_dosage_amounts.add(dosage)
            
            status = med.get("status", "")
            if isinstance(status, str) and status:
                all_statuses.add(status)
        
        # Set the date field (support both new and legacy formats)
        date_field = "date" if "date" in merged else "prescribed_on"
        if len(prescribed_dates) > 1:
            # Most recent date
            merged[date_field] = prescribed_dates[0]  # First item after reverse sort is most recent
            # Full chronological history in prescription_history
            prescribed_dates.reverse()  # Oldest to newest for history
            merged["prescription_history"] = prescribed_dates
        elif len(prescribed_dates) == 1:
            merged[date_field] = prescribed_dates[0]
            # No need for prescription_history if only one date
        
        # If there are multiple different dosage amounts, add history
        if len(all_dosage_amounts) > 1:
            merged["dosage_history"] = sorted(list(all_dosage_amounts))
        
        # Keep most recent status but add note if status changed
        if len(all_statuses) > 1:
            merged["status_history"] = sorted(list(all_statuses))
        
        return merged
 