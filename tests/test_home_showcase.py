import unittest
from unittest.mock import patch

import app.app as site


class HomeShowcaseTests(unittest.TestCase):
    def part(self, identifier, warehouse_name, code=None, qty=1, deleted=False, in_stock=True):
        return site.Part(id=identifier, part_number=code or f"OEM{identifier}", name="Деталь",
                         qty=qty, in_stock=in_stock, is_deleted=deleted, has_photo=False,
                         warehouse=site.Warehouse(name=warehouse_name))

    def test_guaranteed_warehouse_wins_duplicate_oem_and_fills_first(self):
        guaranteed = self.part(1, "Склад гарантованої наявності", code="SAME")
        duplicate = self.part(2, "Tiguan 2", code="SAME", qty=20)
        other = self.part(3, "Tiguan 2")
        result, total = site.build_home_showcase_parts([other, duplicate, guaranteed])
        self.assertEqual(result, [guaranteed, other])
        self.assertEqual(total, 2)

    def test_random_selection_changes_without_mutating_input(self):
        parts = [self.part(i, "Склад гарантованої наявності") for i in range(1, 21)]
        original = parts.copy()
        with patch.object(site.random, "shuffle", side_effect=lambda rows: rows.reverse()):
            first, total = site.build_home_showcase_parts(parts, limit=12)
        with patch.object(site.random, "shuffle", side_effect=lambda rows: None):
            second, _ = site.build_home_showcase_parts(parts, limit=12)
        self.assertNotEqual([p.id for p in first], [p.id for p in second])
        self.assertEqual(total, 20)
        self.assertEqual(len({p.id for p in first}), 12)
        self.assertEqual(parts, original)

    def test_unavailable_items_excluded_and_other_stock_fills_gaps(self):
        parts = [self.part(1, "Склад гарантованої наявності", qty=0),
                 self.part(2, "Склад гарантованої наявності", deleted=True),
                 self.part(3, "Склад гарантованої наявності", in_stock=False),
                 self.part(4, "Tiguan 2")]
        result, total = site.build_home_showcase_parts(parts)
        self.assertEqual([p.id for p in result], [4])
        self.assertEqual(total, 1)


if __name__ == "__main__":
    unittest.main()
