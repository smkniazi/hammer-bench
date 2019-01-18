sudo ceph osd crush set osd.0 1.0 root=default host=gcemaism-cephfs-osd1-1 rack=zone1
sudo ceph osd crush set osd.1 1.0 root=default host=gcemaism-cephfs-osd1-2 rack=zone1
sudo ceph osd crush set osd.2 1.0 root=default host=gcemaism-cephfs-osd1-3 rack=zone1
sudo ceph osd crush set osd.3 1.0 root=default host=gcemaism-cephfs-osd1-4 rack=zone1
sudo ceph osd crush set osd.4 1.0 root=default host=gcemaism-cephfs-osd2-1 rack=zone2
sudo ceph osd crush set osd.5 1.0 root=default host=gcemaism-cephfs-osd2-2 rack=zone2
sudo ceph osd crush set osd.6 1.0 root=default host=gcemaism-cephfs-osd2-3 rack=zone2
sudo ceph osd crush set osd.7 1.0 root=default host=gcemaism-cephfs-osd2-4 rack=zone2
sudo ceph osd crush set osd.8 1.0 root=default host=gcemaism-cephfs-osd3-1 rack=zone3
sudo ceph osd crush set osd.9 1.0 root=default host=gcemaism-cephfs-osd3-2 rack=zone3
sudo ceph osd crush set osd.10 1.0 root=default host=gcemaism-cephfs-osd3-3 rack=zone3
sudo ceph osd crush set osd.11 1.0 root=default host=gcemaism-cephfs-osd3-4 rack=zone3

sudo ceph osd crush rule create-replicated osd-rule default rack
sudo ceph osd pool set cephfs_metadata crush_rule osd-rule
sudo ceph osd pool set cephfs_data crush_rule osd-rule
