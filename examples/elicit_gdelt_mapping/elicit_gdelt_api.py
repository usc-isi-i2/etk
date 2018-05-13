import json
from typing import List
import re


class GdeltMapping(object):

    event_name = {
        "event1": "event1",
        "event2": "event2 (has_topic of event1)",
        "event3": "event3 (has_topic of event2)"
    }

    def __init__(self, elicit_gdelt_mapping):
        self.mapping = elicit_gdelt_mapping

    def has_cameo_code(self, cameo_code) -> bool:
        return cameo_code in self.mapping

    def event_type(self, event, cameo_code) -> List[str]:
        """
        Look up the event tupe of an event
        Args:
            event: one of "event1", "event2" or "event3"
            cameo_code: one of the cameo codes

        Returns: a list of the event types or None if the event is not relevant.

        """
        key = self.event_name[event]
        result = self.mapping[cameo_code][key]
        if result == "":
            return None
        elif not isinstance(result, list):
            result = [result]
        return result

    actor1_regex = re.compile(r'(\w+\:\w+)\sactor1')
    actor2_regex = re.compile(r'(\w+\:\w+)\sactor2')

    def actor_property(self, event, cameo_code, actor_regex):
        """
        Determine the property to use for modeling an actor
        Args:
            event: one of "event1", "event2" or "event3"
            cameo_code: one of the cameo codes
            actor_regex: one of the regexes above

        Returns:

        """
        arguments = self.mapping[cameo_code][event + "-arguments"]
        if not isinstance(arguments, list):
            arguments = [arguments]

        result = list()
        for a in arguments:
            match = re.search(actor_regex, a)
            if match:
                result.append(match.group(1))
        return result[0] if len(result) > 0 else None

    def actor1_property(self, event, cameo_code):
        return self.actor_property(event, cameo_code, self.actor1_regex)

    def actor2_property(self, event, cameo_code):
        return self.actor_property(event, cameo_code, self.actor2_regex)


if __name__ == "__main__":
    gdelt = GdeltMapping(json.load(open("ODP-Mappings-V3.1.json")))
    # print(gdelt.event_type("event1", "010"))
    for k in gdelt.mapping.keys():
        # print("{}: {}".format(k, gdelt.event_type("event1", k)))
        print("{}: {}".format(k, gdelt.actor2_property("event1", k)))

