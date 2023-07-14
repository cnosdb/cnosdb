use std::error::Error;
use std::u64;


pub fn f64_sdt_encode(
    src: &[f64],
    dst: &mut Vec<u8>,
    deviation:f64
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer.


    if src.is_empty() {
        return Ok(());
    }
    let mut prev_upper_slope = 0.0;
    let mut prev_lower_slope = 0.0;
    let mut home = 0;//store the index of the number which opens the door
    let mut first = src[0];//the number which opens the door
    println!("{}",first);
    let mut prev_value = first.to_bits();
    dst.extend_from_slice(&first.to_be_bytes()); //add the first one

    for i in 1..src.len() {

        let value = src[i];
        let check = (i-home) as u32;
        //println!("value:{}",value);
        if  check == 1 {
            prev_upper_slope = value + deviation - first;
            prev_lower_slope = value - deviation - first;
        }

        //println!("{},{}",prev_upper_slope,prev_lower_slope);
        let offset = (i-home) as f64;

        let slope = value/offset;

        if slope <= prev_upper_slope && slope >= prev_lower_slope{
            let upper_slope = (value+deviation-first)/offset;
            let lower_slope = (value-deviation-first)/offset;
            if upper_slope < prev_upper_slope {prev_upper_slope = upper_slope;}
            if lower_slope > prev_lower_slope {prev_lower_slope = lower_slope;}
            continue;
        }

        home = i-1;


        first = src[home];
        println!("{}",home);
        println!("{}",first);
        prev_value = first.to_bits();
        dst.extend_from_slice(&prev_value.to_be_bytes());
        let home_u16 = home as u16;
        dst.extend_from_slice(&home_u16.to_be_bytes());
        prev_upper_slope = value + deviation - first;
        prev_lower_slope = value - deviation - first;


    }

    first = src[src.len()-1];
    //println!("end:{}",first);
    dst.extend_from_slice(&first.to_be_bytes());
    let len = (src.len() - 1)  as u16;
    //println!("{}",len);
    dst.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

pub fn u64_sdt_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
    deviation:f64
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer.


    if src.is_empty() {
        return Ok(());
    }
    let mut prev_upper_slope = 0.0;
    let mut prev_lower_slope = 0.0;
    let mut home = 0;//store the index of the number which opens the door
    let mut first_u64 = src[0] ;//the number which opens the door
    //println!("{}",first_u64);
    let mut prev_value = first_u64;
    dst.extend_from_slice(&first_u64.to_be_bytes()); //add the first one
    let mut first = first_u64 as f64;
    //println!("{}",first);
    let mut adfirst = 0;
    for i in 1..src.len() {

        let value = src[i] as f64;
        let check = (i-home) as u32;
        //println!("value:{}",value);
        if  check == 1 {
            prev_upper_slope = value + deviation - first;
            prev_lower_slope = value - deviation - first;
        }

        //println!("{},{}",prev_upper_slope,prev_lower_slope);
        let offset = (i-home) as f64;

        let slope = value/offset;

        if slope <= prev_upper_slope && slope >= prev_lower_slope{
            let upper_slope = (value+deviation-first)/offset;
            let lower_slope = (value-deviation-first)/offset;
            if upper_slope < prev_upper_slope {prev_upper_slope = upper_slope;}
            if lower_slope > prev_lower_slope {prev_lower_slope = lower_slope;}
            continue;
        }

        home = i-1;



        adfirst = src[home];
        first = adfirst as f64;
        println!("home:{}",home);
        println!("first:{}",first);
        prev_value = adfirst;
        dst.extend_from_slice(&prev_value.to_be_bytes());
        let home_u16 = home as u16;
        dst.extend_from_slice(&home_u16.to_be_bytes());
        prev_upper_slope = value + deviation - first;
        prev_lower_slope = value - deviation - first;


    }

    first = src[src.len()-1] as f64;
    adfirst = src[src.len()-1];
    //println!("end:{}",first);
    dst.extend_from_slice(&adfirst.to_be_bytes());
    let len = (src.len() - 1)  as u16;
    //println!("{}",len);
    dst.extend_from_slice(&len.to_be_bytes());
    Ok(())
}


pub fn f64_sdt_decode(
    src: &[u8],
    dst: &mut Vec<f64>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.len() < 8 {

        return Ok(());
    }


    let mut i = 0; //
    let mut buf: [u8; 8] = [0; 8];
    let mut interval: [u8;2] = [0;2];
    let mut val_first:u64;
    let mut val:u64;
    let mut home_first:u16;
    let mut home:u16;
    let mut val_first_f64:f64;
    let mut val_f64:f64;
    let mut differ:f64;
    let mut offset:u16;
    let mut slope:f64;
    let mut dec:f64;
    //incase the src is short
    println!("len:{}",src.len());
    if (src.len() - i) <= 10 {
        buf.copy_from_slice(&src[i..i + 8]);
        val_first = u64::from_be_bytes(buf); //as f64
        val_first_f64 = f64::from_bits(val_first);
        println!("{}",val_first_f64);
        dst.push(val_first_f64);
        return Ok(());
    }
    //the first one

    buf.copy_from_slice(&src[i..i + 8]);
    val_first = u64::from_be_bytes(buf);
    val_first_f64 = f64::from_bits(val_first);
    //println!("{}",val_first_f64);
    i += 8;
    home_first = 0 ;
    loop{
        //the last one
        let mut cnt = 0;
        //println!("sricnt:{}",cnt);
        if src.len() - i <= 10 {
            dst.push(val_first_f64);
            //println!("the last");
            //println!("end:{}",val_first_f64);
            //println!("{}",i);
            buf.copy_from_slice(&src[i..i + 8]);
            interval.copy_from_slice(&src[i+8..i+10]);
            val = u64::from_be_bytes(buf);
            val_f64 = f64::from_bits(val);

            home = u16::from_be_bytes(interval);
            offset = home - home_first + 1;
            //println!("last:{}",offset);
            let mut ss = 0;
            if offset > 2 {

                dec = val_first_f64;
                differ = val_f64 - val_first_f64;
                for k in 0..(offset-2){
                    ss+=1;
                    slope = differ / offset as f64;
                    dec += slope;
                    dst.push(dec);
                }
            }
            //println!("ss:{}",ss);
            dst.push(val_f64);
            break;
        }
        dst.push(val_first_f64);

        buf.copy_from_slice(&src[i..i + 8]);
        val = u64::from_be_bytes(buf);
        val_f64 = f64::from_bits(val);
        //println!("val:{}",val_f64);

        interval.copy_from_slice(&src[i + 8..i + 10]);
        home = u16::from_be_bytes(interval);
        //println!("{}",home);
        offset = home - home_first + 1;
        //println!("{}",offset);
        differ = val_f64 - val_first_f64;
        dec = val_first_f64;
        //println!("val_first_64:{}",dec);
        if offset > 2{
            slope = differ / offset as f64;
            for mut j in 0..(offset-2){
                //cnt += 1;

                //println!("slope:{}",slope);
                dec = dec + slope;//as u64  6.3 -> 6
                //println!("dec:{}",dec);
                dst.push(dec);
                j += 1;
            }
        }

        //println!("cnt:{}",cnt);
        val_first_f64 = val_f64;
        //println!("val_first_64:{}",val_first_f64);
        home_first = home;
        i += 10;

    }




    Ok(())
}


pub fn u64_sdt_decode(
    src: &[u8],
    dst: &mut Vec<u64>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.len() < 8 {

        return Ok(());
    }


    let mut i = 0; //
    let mut buf: [u8; 8] = [0; 8];
    let mut interval: [u8;2] = [0;2];
    let mut val_first:u64;
    let mut val:u64;
    let mut home_first:u16;
    let mut home:u16;
    let mut val_first_f64:f64;
    let mut val_f64:f64;
    let mut differ:f64;
    let mut offset:u16;
    let mut slope:f64;
    let mut dec:f64;
    let mut addec;
    //incase the src is short
    println!("len:{}",src.len());
    if (src.len() - i) <= 10 {
        buf.copy_from_slice(&src[i..i + 8]);
        val_first = u64::from_be_bytes(buf); //as f64
        //val_first_f64 = f64::from_bits(val_first);
        println!("{}",val_first);
        dst.push(val_first);
        return Ok(());
    }
    //the first one

    buf.copy_from_slice(&src[i..i + 8]);
    val_first = u64::from_be_bytes(buf);
    val_first_f64 = val_first as f64;
    println!("{}",val_first);
    i += 8;
    home_first = 0 ;
    loop{
        //the last one
        //let mut cnt = 0;
        //println!("sricnt:{}",cnt);
        if src.len() - i <= 10 {
            dst.push(val_first);
            println!("the last");
            println!("end:{}",val_first);
            //println!("{}",i);
            buf.copy_from_slice(&src[i..i + 8]);
            interval.copy_from_slice(&src[i+8..i+10]);
            val = u64::from_be_bytes(buf);
            val_f64 = val as f64;

            home = u16::from_be_bytes(interval);
            offset = home - home_first + 1;
            println!("last:{}",offset);
            let mut ss = 0;
            if offset > 2 {

                dec = val_first as f64;
                differ = val_f64 - dec;
                for k in 0..(offset-2){
                    ss+=1;
                    slope = differ / offset as f64;
                    dec += slope;
                    addec = dec as u64;
                    dst.push(addec);
                }
            }
            println!("ss:{}",ss);
            dst.push(val);
            break;
        }
        dst.push(val_first);

        buf.copy_from_slice(&src[i..i + 8]);
        val = u64::from_be_bytes(buf);
        val_f64 = val as f64;
        println!("val:{}",val_f64);

        interval.copy_from_slice(&src[i + 8..i + 10]);
        home = u16::from_be_bytes(interval);
        //println!("{}",home);
        offset = home - home_first + 1;
        //println!("offset:{}",offset);
        differ = val_f64 - val_first_f64;
        dec = val_first_f64;
        println!("val_first_64:{}",dec);
        if offset > 2{
            slope = differ / offset as f64;
            for mut j in 0..(offset-2){
                //cnt += 1;

                println!("slope:{}",slope);
                dec = dec + slope;//as u64  6.3 -> 6
                println!("dec:{}",dec);
                addec = dec as u64;
                println!("addec:{}",dec);
                dst.push(addec);
                j += 1;
            }
        }

        //println!("cnt:{}",cnt);
        val_first_f64 = val_f64;
        val_first = val;
        println!("val_first_64:{}",val_first_f64);
        home_first = home;
        i += 10;

    }




    Ok(())
}
