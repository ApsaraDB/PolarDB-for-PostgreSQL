DROP TABLE IF EXISTS twophase;
CREATE TABLE twophase (v int);
BEGIN;
	INSERT INTO twophase VALUES(10000);
	SELECT MAX(v) FROM twophase;
PREPARE TRANSACTION 'twophase_test';
	
	INSERT INTO twophase VALUES(1);
	INSERT INTO twophase VALUES(2);
	INSERT INTO twophase VALUES(3);
	INSERT INTO twophase VALUES(4);
	INSERT INTO twophase VALUES(5);
	INSERT INTO twophase VALUES(6);
	INSERT INTO twophase VALUES(7);
	INSERT INTO twophase VALUES(8);
	INSERT INTO twophase VALUES(9);
	INSERT INTO twophase VALUES(10);
	INSERT INTO twophase VALUES(11);
	INSERT INTO twophase VALUES(12);
	INSERT INTO twophase VALUES(13);
	INSERT INTO twophase VALUES(14);
	INSERT INTO twophase VALUES(15);
	INSERT INTO twophase VALUES(16);
	INSERT INTO twophase VALUES(17);
	INSERT INTO twophase VALUES(18);
	INSERT INTO twophase VALUES(19);
	INSERT INTO twophase VALUES(20);
	INSERT INTO twophase VALUES(21);
	INSERT INTO twophase VALUES(22);
	INSERT INTO twophase VALUES(23);
	INSERT INTO twophase VALUES(24);
	INSERT INTO twophase VALUES(25);
	INSERT INTO twophase VALUES(26);
	INSERT INTO twophase VALUES(27);
	INSERT INTO twophase VALUES(28);
	INSERT INTO twophase VALUES(29);
	INSERT INTO twophase VALUES(30);
	INSERT INTO twophase VALUES(31);
	INSERT INTO twophase VALUES(32);
	INSERT INTO twophase VALUES(33);
	INSERT INTO twophase VALUES(34);
	INSERT INTO twophase VALUES(35);
	INSERT INTO twophase VALUES(36);
	INSERT INTO twophase VALUES(37);
	INSERT INTO twophase VALUES(38);
	INSERT INTO twophase VALUES(39);
	INSERT INTO twophase VALUES(40);
	INSERT INTO twophase VALUES(41);
	INSERT INTO twophase VALUES(42);
	INSERT INTO twophase VALUES(43);
	INSERT INTO twophase VALUES(44);
	INSERT INTO twophase VALUES(45);
	INSERT INTO twophase VALUES(46);
	INSERT INTO twophase VALUES(47);
	INSERT INTO twophase VALUES(48);
	INSERT INTO twophase VALUES(49);
	INSERT INTO twophase VALUES(50);
	INSERT INTO twophase VALUES(51);
	INSERT INTO twophase VALUES(52);
	INSERT INTO twophase VALUES(53);
	INSERT INTO twophase VALUES(54);
	INSERT INTO twophase VALUES(55);
	INSERT INTO twophase VALUES(56);
	INSERT INTO twophase VALUES(57);
	INSERT INTO twophase VALUES(58);
	INSERT INTO twophase VALUES(59);
	INSERT INTO twophase VALUES(60);
	INSERT INTO twophase VALUES(61);
	INSERT INTO twophase VALUES(62);
	INSERT INTO twophase VALUES(63);
	INSERT INTO twophase VALUES(64);
	INSERT INTO twophase VALUES(65);
	INSERT INTO twophase VALUES(66);
	INSERT INTO twophase VALUES(67);
	INSERT INTO twophase VALUES(68);
	INSERT INTO twophase VALUES(69);
	INSERT INTO twophase VALUES(70);
	INSERT INTO twophase VALUES(71);
	INSERT INTO twophase VALUES(72);
	INSERT INTO twophase VALUES(73);
	INSERT INTO twophase VALUES(74);
	INSERT INTO twophase VALUES(75);
	INSERT INTO twophase VALUES(76);
	INSERT INTO twophase VALUES(77);
	INSERT INTO twophase VALUES(78);
	INSERT INTO twophase VALUES(79);
	INSERT INTO twophase VALUES(80);
	INSERT INTO twophase VALUES(81);
	INSERT INTO twophase VALUES(82);
	INSERT INTO twophase VALUES(83);
	INSERT INTO twophase VALUES(84);
	INSERT INTO twophase VALUES(85);
	INSERT INTO twophase VALUES(86);
	INSERT INTO twophase VALUES(87);
	INSERT INTO twophase VALUES(88);
	INSERT INTO twophase VALUES(89);
	INSERT INTO twophase VALUES(90);
