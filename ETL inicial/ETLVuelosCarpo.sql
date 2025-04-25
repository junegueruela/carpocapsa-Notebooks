/* Una vez cargados los vuelos en la tabla temporal vuelos, normalizamos esta información y la cargamos
en VuelosCarpo. Para ello, borramos las entradas a nulo primero. No las convertimos a un valor a 0, porque  el 0 
es significativo, significa que no ha habido vuelos y es algo a tener en cuenta por algoritmo */
delete from Vuelos where valor is null;
update Vuelos set idMunicipio=(select idMunicipio from Terminos where Terminos.termino=Vuelos.termino);
insert into VuelosCarpo (fecha, idTermino, valor) 
 ( select fecha, idTermino, valor from Vuelos v, Terminos t
 where t.Termino=v.termino);
  

