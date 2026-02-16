export function roundToHour(date: Date) {
    const d = new Date(date);
    d.setUTCMinutes(0, 0, 0);
    return d;
}
