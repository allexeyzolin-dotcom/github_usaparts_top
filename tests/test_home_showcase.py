import unittest
from unittest.mock import patch

import app.app as site


class HomeShowcaseTests(unittest.TestCase):
    def part(self, identifier, warehouse_name, code=None, qty=1, deleted=False, in_stock=True,
             photo="https://example.com/product.jpg"):
        return site.Part(id=identifier, part_number=code or f"OEM{identifier}", name="Деталь",
                         qty=qty, in_stock=in_stock, is_deleted=deleted, has_photo=False,
                         photo_urls=photo, showcase_photo_urls="",
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

    def test_only_attached_photos_qualify_in_both_warehouses(self):
        no_photo = self.part(1, "Склад гарантованої наявності", photo="")
        no_photo.has_photo = True
        placeholder = self.part(2, "Склад гарантованої наявності", photo=site.PUBLIC_PART_PLACEHOLDER_IMAGE)
        other_missing = self.part(3, "Tiguan 2", photo="")
        real_photo = self.part(4, "Tiguan 2")
        result, total = site.build_home_showcase_parts([no_photo, placeholder, other_missing, real_photo])
        self.assertEqual([p.id for p in result], [4])
        self.assertEqual(total, 4)  # The catalog count still includes products without photos.

    def test_gallery_photo_qualifies_and_photographed_duplicate_is_kept(self):
        missing = self.part(1, "Склад гарантованої наявності", code="SAME", photo="")
        gallery = self.part(2, "Tiguan 2", code="SAME", photo="")
        gallery.showcase_photo_urls = "https://example.com/gallery.jpg"
        result, total = site.build_home_showcase_parts([missing, gallery])
        self.assertEqual(result, [gallery])
        self.assertEqual(total, 1)


if __name__ == "__main__":
    unittest.main()
